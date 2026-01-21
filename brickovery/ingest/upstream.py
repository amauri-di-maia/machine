from __future__ import annotations

from pathlib import Path
import sqlite3
import urllib.parse
from typing import Dict, Any, List, Tuple, Optional
import hashlib

from ..io.hashing import sha256_file
from ..io.timeutil import utc_now_iso
from ..github_sync.git import head_sha


def _sqlite_ro_uri(path: str) -> str:
    p = Path(path).resolve().as_posix()
    enc = urllib.parse.quote(p)
    return f"file:{enc}?mode=ro"


def _list_tables(con: sqlite3.Connection) -> List[str]:
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    return [r[0] for r in cur.fetchall()]


def _table_cols(con: sqlite3.Connection, table: str) -> List[str]:
    cur = con.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]


def _score_mapping_table(cols: List[str]) -> int:
    low = [c.lower() for c in cols]
    score = 0
    if any(("bricklink" in c) or ("bl_" in c) or ("part_id" in c) or ("partno" in c) for c in low):
        score += 3
    if any(("color" in c) for c in low):
        score += 2
    if any(("boid" in c) or ("brickowl" in c) for c in low):
        score += 3
    if any(("itemtype" in c) or (c == "type") or ("item_type" in c) for c in low):
        score += 1
    if any(("weight" in c) or ("grams" in c) or ("mass" in c) for c in low):
        score += 1
    return score


def _choose_best_table(con: sqlite3.Connection) -> Tuple[str, Dict[str, str]]:
    tables = _list_tables(con)
    candidates: List[Tuple[int, str, List[str]]] = []
    for t in tables:
        cols = _table_cols(con, t)
        s = _score_mapping_table(cols)
        if s >= 6:
            candidates.append((s, t, cols))
    if not candidates:
        raise RuntimeError(
            "Could not auto-detect mapping table in upstream DB. "
            "Provide schema_hint.mapping_table and column names in config."
        )
    candidates.sort(reverse=True, key=lambda x: (x[0], x[1]))
    top_score = candidates[0][0]
    best = [c for c in candidates if c[0] == top_score]
    if len(best) > 1:
        msg = "Ambiguous mapping table detection. Top candidates:\n"
        for s, t, cols in best[:10]:
            msg += f"- {t} (score={s}) cols={cols}\n"
        raise RuntimeError(msg)

    _, table, cols = candidates[0]

    def pick(keys: List[str]) -> Optional[str]:
        for c in cols:
            cl = c.lower()
            if any(k in cl for k in keys):
                return c
        return None

    mapping = {
        "mapping_table": table,
        "bl_part_col": pick(["bl_part", "bricklink", "part_id", "bl_id", "item_id", "partno", "bl_part_id"]),
        "bl_color_col": pick(["bl_color", "bricklink_color", "color", "bl_color_id"]),
        "bl_itemtype_col": pick(["itemtype", "item_type", "type"]),
        "bo_boid_col": pick(["boid", "brickowl", "bo_boid"]),
        "weight_g_col": pick(["weight", "grams", "mass"]),
    }
    if not mapping["bl_part_col"] or not mapping["bo_boid_col"]:
        raise RuntimeError(f"Auto-detected table {table} but missing required columns. Detection={mapping}, cols={cols}")
    return table, mapping


def _row_hash(*vals: Any) -> str:
    h = hashlib.sha256()
    for v in vals:
        h.update(str(v).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


def _normalize_boid(v: Any) -> Optional[str]:
    """
    BOID no upstream pode ser TEXT (ex.: '656416-20').
    Nunca converter para int: preservar string canónica.
    """
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def ingest_upstream_mapping(
    con_work: sqlite3.Connection,
    upstream_checkout_path: str,
    upstream_db_relpath: str,
    upstream_repo: str,
    upstream_ref: str,
    schema_hint: Dict[str, Any],
) -> Dict[str, Any]:
    upstream_root = Path(upstream_checkout_path)
    db_path = upstream_root / upstream_db_relpath
    if not db_path.exists():
        raise FileNotFoundError(f"Upstream DB not found at: {db_path}")

    upstream_commit_sha = head_sha(str(upstream_root))
    upstream_db_sha256 = sha256_file(str(db_path))

    ro_uri = _sqlite_ro_uri(str(db_path))
    con_up = sqlite3.connect(ro_uri, uri=True)
    con_up.row_factory = sqlite3.Row

    hint = schema_hint or {}
    mapping_table = hint.get("mapping_table")
    if mapping_table:
        mapping_cols = {
            "mapping_table": mapping_table,
            "bl_part_col": hint.get("bl_part_col"),
            "bl_color_col": hint.get("bl_color_col"),
            "bl_itemtype_col": hint.get("bl_itemtype_col"),
            "bo_boid_col": hint.get("bo_boid_col"),
            "weight_g_col": hint.get("weight_g_col"),
        }
        if not mapping_cols["bl_part_col"] or not mapping_cols["bo_boid_col"]:
            raise RuntimeError("schema_hint provided but missing required columns.")
    else:
        mapping_table, mapping_cols = _choose_best_table(con_up)

    sel_cols = [
        f"{mapping_cols['bl_part_col']} AS bl_part_id",
        f"{mapping_cols['bo_boid_col']} AS bo_boid",
    ]
    if mapping_cols.get("bl_color_col"):
        sel_cols.append(f"{mapping_cols['bl_color_col']} AS bl_color_id")
    else:
        sel_cols.append("NULL AS bl_color_id")
    if mapping_cols.get("bl_itemtype_col"):
        sel_cols.append(f"{mapping_cols['bl_itemtype_col']} AS bl_itemtype")
    else:
        sel_cols.append("'P' AS bl_itemtype")
    if mapping_cols.get("weight_g_col"):
        sel_cols.append(f"{mapping_cols['weight_g_col']} AS weight_g")
    else:
        sel_cols.append("NULL AS weight_g")

    query = f"SELECT {', '.join(sel_cols)} FROM {mapping_table};"
    rows = con_up.execute(query).fetchall()

    ts = utc_now_iso()
    con_work.execute("DELETE FROM up_mapping_mirror;")
    inserted = 0
    null_color = 0
    null_weight = 0
    null_boid = 0

    for r in rows:
        itemtype = str(r["bl_itemtype"]).strip() if r["bl_itemtype"] is not None else "P"
        part_id = str(r["bl_part_id"]).strip()

        color_id = r["bl_color_id"]
        try:
            color_id_int = int(color_id) if color_id is not None else None
        except Exception:
            color_id_int = None

        # BOID: preservar string (pode conter '-')
        boid_txt = _normalize_boid(r["bo_boid"])

        # Peso: assumir que upstream está em gramas (float/int) e converter para mg
        weight_mg = None
        if r["weight_g"] is not None:
            try:
                weight_mg = int(round(float(r["weight_g"]) * 1000.0))
            except Exception:
                weight_mg = None

        if color_id_int is None:
            null_color += 1
        if boid_txt is None:
            null_boid += 1
        if weight_mg is None:
            null_weight += 1

        rh = _row_hash(itemtype, part_id, color_id_int, boid_txt, weight_mg)

        con_work.execute(
            "INSERT OR REPLACE INTO up_mapping_mirror("
            "bl_itemtype, bl_part_id, bl_color_id, bo_boid, weight_mg, source, row_hash, ingested_ts"
            ") VALUES(?,?,?,?,?,?,?,?);",
            (itemtype, part_id, color_id_int, boid_txt, weight_mg, "upstream", rh, ts),
        )
        inserted += 1

    con_work.execute("DELETE FROM upstream_state WHERE id=1;")
    con_work.execute(
        "INSERT INTO upstream_state("
        "id, upstream_repo, upstream_ref, upstream_commit_sha, upstream_db_relpath, upstream_db_sha256, ingested_ts"
        ") VALUES(1,?,?,?,?,?,?);",
        (upstream_repo, upstream_ref, upstream_commit_sha, upstream_db_relpath, upstream_db_sha256, ts),
    )
    con_work.commit()
    con_up.close()

    return {
        "upstream_commit_sha": upstream_commit_sha,
        "upstream_db_sha256": upstream_db_sha256,
        "mapping_table_used": mapping_table,
        "rows_inserted": inserted,
        "rows_null_color": null_color,
        "rows_null_boid": null_boid,
        "rows_null_weight": null_weight,
        "mapping_detection": mapping_cols,
    }
