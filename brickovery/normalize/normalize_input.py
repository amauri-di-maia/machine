from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
import sqlite3

from ..io.timeutil import utc_now_iso
from ..io.hashing import sha256_json
from ..io.search_xml import RawSearchItem


@dataclass(frozen=True)
class RequestedItem:
    bl_itemtype: str
    bl_part_id: str
    bl_color_id: Optional[int]
    condition: str
    qty: int


@dataclass(frozen=True)
class NormalizedItem:
    bl_itemtype: str
    bl_part_id: str
    bl_color_id: Optional[int]
    condition: str
    qty: int

    bo_boid: Optional[int]
    weight_mg: Optional[int]

    mapping_source: str            # "override" | "upstream" | "none"
    mapping_status: str            # "OK" | "MISSING_BOID"
    weight_status: str             # "OK" | "MISSING_WEIGHT"


def _color_match_sql() -> str:
    # comparador robusto para NULL
    return "((bl_color_id IS NULL AND ? IS NULL) OR (bl_color_id = ?))"


def aggregate_requested(raw: List[RawSearchItem]) -> List[RequestedItem]:
    agg: Dict[Tuple[str, str, Optional[int], str], int] = {}
    for r in raw:
        key = (r.bl_itemtype, r.bl_part_id, r.bl_color_id, r.condition)
        agg[key] = agg.get(key, 0) + r.qty

    out = [
        RequestedItem(
            bl_itemtype=k[0],
            bl_part_id=k[1],
            bl_color_id=k[2],
            condition=k[3],
            qty=v,
        )
        for k, v in agg.items()
    ]

    # Ordenação determinística
    out.sort(key=lambda x: (x.bl_itemtype, x.bl_part_id, -1 if x.bl_color_id is None else x.bl_color_id, x.condition))
    return out


def _lookup_override(con: sqlite3.Connection, it: RequestedItem) -> Optional[sqlite3.Row]:
    q = f"""
      SELECT bo_boid, status
      FROM mapping_overrides
      WHERE bl_itemtype = ?
        AND bl_part_id = ?
        AND {_color_match_sql()}
      LIMIT 1;
    """
    return con.execute(q, (it.bl_itemtype, it.bl_part_id, it.bl_color_id, it.bl_color_id)).fetchone()


def _lookup_upstream_exact(con: sqlite3.Connection, it: RequestedItem) -> Optional[sqlite3.Row]:
    q = f"""
      SELECT bo_boid, weight_mg
      FROM up_mapping_mirror
      WHERE bl_itemtype = ?
        AND bl_part_id = ?
        AND {_color_match_sql()}
      LIMIT 1;
    """
    return con.execute(q, (it.bl_itemtype, it.bl_part_id, it.bl_color_id, it.bl_color_id)).fetchone()


def _lookup_weight_fallback(con: sqlite3.Connection, it: RequestedItem) -> Optional[int]:
    q = """
      SELECT weight_mg
      FROM up_mapping_mirror
      WHERE bl_itemtype = ?
        AND bl_part_id = ?
        AND weight_mg IS NOT NULL
        AND weight_mg > 0
      LIMIT 1;
    """
    row = con.execute(q, (it.bl_itemtype, it.bl_part_id)).fetchone()
    if row is None:
        return None
    return int(row[0]) if row[0] is not None else None


def _ensure_fill_later_override(con: sqlite3.Connection, it: RequestedItem, reason: str) -> bool:
    # idempotente: não cria duplicados
    now = utc_now_iso()
    q = """
      INSERT OR IGNORE INTO mapping_overrides
        (bl_itemtype, bl_part_id, bl_color_id, bo_boid, status, reason, created_ts, updated_ts)
      VALUES
        (?, ?, ?, NULL, 'fill_later', ?, ?, ?);
    """
    cur = con.execute(q, (it.bl_itemtype, it.bl_part_id, it.bl_color_id, reason, now, now))
    return cur.rowcount == 1


def normalize_items(con: sqlite3.Connection, requested: List[RequestedItem]) -> Tuple[List[NormalizedItem], Dict[str, int], int]:
    stats = {
        "requested_items": len(requested),
        "mapped_ok": 0,
        "missing_boid": 0,
        "weight_ok": 0,
        "missing_weight": 0,
        "overrides_created": 0,
    }
    total_weight_mg = 0

    out: List[NormalizedItem] = []
    for it in requested:
        # 1) override tem prioridade (se tiver boid)
        ov = _lookup_override(con, it)
        bo_boid: Optional[int] = None
        mapping_source = "none"

        if ov is not None and ov["bo_boid"] is not None:
            bo_boid = int(ov["bo_boid"])
            mapping_source = "override"

        # 2) se não houver override resolvido, tenta upstream exato (part+color)
        up = _lookup_upstream_exact(con, it)
        weight_mg: Optional[int] = None
        if mapping_source == "none" and up is not None and up["bo_boid"] is not None:
            bo_boid = int(up["bo_boid"])
            mapping_source = "upstream"

        if up is not None and up["weight_mg"] is not None:
            weight_mg = int(up["weight_mg"])

        # 3) fallback de weight por part (cor pode falhar mas peso não depende da cor)
        if weight_mg is None:
            weight_mg = _lookup_weight_fallback(con, it)

        mapping_status = "OK" if bo_boid is not None else "MISSING_BOID"
        weight_status = "OK" if (weight_mg is not None and weight_mg > 0) else "MISSING_WEIGHT"

        if mapping_status == "OK":
            stats["mapped_ok"] += 1
        else:
            stats["missing_boid"] += 1
            created = _ensure_fill_later_override(con, it, reason="missing boid in upstream/override")
            if created:
                stats["overrides_created"] += 1

        if weight_status == "OK":
            stats["weight_ok"] += 1
            total_weight_mg += int(weight_mg) * int(it.qty)
        else:
            stats["missing_weight"] += 1

        out.append(
            NormalizedItem(
                bl_itemtype=it.bl_itemtype,
                bl_part_id=it.bl_part_id,
                bl_color_id=it.bl_color_id,
                condition=it.condition,
                qty=it.qty,
                bo_boid=bo_boid,
                weight_mg=weight_mg,
                mapping_source=mapping_source,
                mapping_status=mapping_status,
                weight_status=weight_status,
            )
        )

    # persist overrides criadas
    con.commit()

    # ordenação determinística
    out.sort(key=lambda x: (x.bl_itemtype, x.bl_part_id, -1 if x.bl_color_id is None else x.bl_color_id, x.condition))
    return out, stats, total_weight_mg


def normalized_items_hash(items: List[NormalizedItem]) -> str:
    # hash determinístico do conteúdo
    payload = [asdict(x) for x in items]
    return sha256_json({"items": payload})
