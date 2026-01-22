from __future__ import annotations

import argparse
import glob
import json
import os
import re
import time
import math
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config.loader import load_config
from .repositories.db import connect, migrate
from .github_sync.lock import RepoLock, RepoLockError
from .github_sync.sync import sync_repo
from .logging.logger import JsonlLogger
from .io.hashing import sha256_json, sha256_file
from .io.timeutil import make_run_id, utc_now_iso
from .io.manifest import RunManifest, write_manifest

from .ingest.upstream import ingest_upstream_mapping
from .ingest.shipping import ingest_shipping_bands
from .ingest.rarity_rules import ingest_rarity_rules


# =========================
# Helpers
# =========================

def _ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _write_json(path: str, obj: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _hash_list(obj: Any) -> str:
    # hash estável (ordenado) para listas/dicts
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    import hashlib
    h = hashlib.sha256()
    h.update(s)
    return h.hexdigest()


def _get_upstream_state(con) -> Dict[str, str]:
    """
    Obtém metadados do upstream (quando disponíveis) para auditoria.
    """
    try:
        row = con.execute(
            """
            SELECT upstream_repo, upstream_ref, upstream_commit_sha, upstream_db_relpath, upstream_db_sha256
            FROM upstream_state WHERE id=1;
            """
        ).fetchone()
        if row:
            return {
                "upstream_repo": row[0] or "",
                "upstream_ref": row[1] or "",
                "upstream_commit_sha": row[2] or "",
                "upstream_db_relpath": row[3] or "",
                "upstream_db_sha256": row[4] or "",
            }
    except Exception:
        pass
    return {
        "upstream_repo": "",
        "upstream_ref": "",
        "upstream_commit_sha": "",
        "upstream_db_relpath": "",
        "upstream_db_sha256": "",
    }


def _table_cols(con, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info({table});").fetchall()
    return [r[1] for r in rows]


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cl = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cl:
            return cl[cand.lower()]
    return None


# =========================
# M0: bootstrap
# =========================

def cmd_bootstrap(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id

    logger = JsonlLogger(cfg.paths.logs_dir, run_id, slice_id)

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    try:
        lock.acquire(owner_run_id=run_id, owner_info={"slice_id": slice_id, "action": "bootstrap"}, wait_seconds=args.lock_wait_seconds)
    except RepoLockError as e:
        raise SystemExit(str(e))

    try:
        logger.log("bootstrap.start", config_path=args.config)

        _ensure_dir(cfg.paths.manifests_dir)
        _ensure_dir(cfg.paths.logs_dir)
        Path(cfg.paths.sqlite_db).parent.mkdir(parents=True, exist_ok=True)

        # opcional: pull
        if getattr(cfg.github_sync, "enabled", False) and getattr(cfg.github_sync, "pull", False) and not args.no_pull:
            logger.log("git.pull.start")
            sync_repo(
                repo_root=cfg.paths.repo_root,
                do_pull=True,
                do_commit=False,
                do_push=False,
                commit_message="brickovery: pull-only",
                user_name=getattr(cfg.github_sync, "git_user_name", "github-actions[bot]"),
                user_email=getattr(cfg.github_sync, "git_user_email", "41898282+github-actions[bot]@users.noreply.github.com"),
                fail_safe_on_push_conflict=getattr(cfg.github_sync, "fail_safe_on_push_conflict", True),
            )
            logger.log("git.pull.done")

        con = connect(cfg.paths.sqlite_db)
        migrate(con)
        logger.log("db.migrated", db_path=cfg.paths.sqlite_db)
        # Ingest upstream mapping (mirror + overrides tables, upstream_state)
        raw_hint = getattr(getattr(cfg, "upstream", None), "schema_hint", None)
        if raw_hint is None:
            schema_hint = {}
        elif isinstance(raw_hint, dict):
            schema_hint = raw_hint
        elif hasattr(raw_hint, "model_dump"):
            schema_hint = raw_hint.model_dump()
        elif hasattr(raw_hint, "dict"):
            schema_hint = raw_hint.dict()
        else:
            try:
                schema_hint = dict(raw_hint)
            except Exception:
                schema_hint = {}

        upstream_checkout_path = (
            getattr(args, "upstream_checkout_path", None)
            or getattr(getattr(cfg, "upstream", None), "checkout_path", None)
            or "upstream/amauri-repo"
        )

        up_res = ingest_upstream_mapping(
            con_work=con,
            upstream_checkout_path=upstream_checkout_path,
            upstream_db_relpath=cfg.upstream.db_relpath,
            upstream_repo=cfg.upstream.repo,
            upstream_ref=cfg.upstream.ref,
            schema_hint=schema_hint,
        )
        logger.log("upstream.ingested", **up_res)

        # Shipping
        sn_hash, sn_n = ingest_shipping_bands(con, cfg.tables.shipping_normal_txt, service_level="normal", dest_country="PT")
        logger.log("shipping.ingested", service_level="normal", bands=sn_n, source_hash=sn_hash)

        sr_hash, sr_n = ingest_shipping_bands(con, cfg.tables.shipping_registered_txt, service_level="registered", dest_country="PT")
        logger.log("shipping.ingested", service_level="registered", bands=sr_n, source_hash=sr_hash)

        # Rarity rules
        rr_hash, rr_n = ingest_rarity_rules(con, cfg.tables.rarity_rules_txt)
        logger.log("rarity.ingested", rules=rr_n, source_hash=rr_hash)

        cfg_hash = sha256_json(cfg.raw if hasattr(cfg, "raw") else cfg.model_dump())  # stable-ish
        manifest = RunManifest(
            run_id=run_id,
            slice_id=slice_id,
            created_ts=utc_now_iso(),
            config_hash=cfg_hash,
            upstream_repo=cfg.upstream.repo,
            upstream_ref=cfg.upstream.ref,
            upstream_commit_sha=up_res.get("upstream_commit_sha", ""),
            upstream_db_relpath=cfg.upstream.db_relpath,
            upstream_db_sha256=up_res.get("upstream_db_sha256", ""),
            notes="M0 bootstrap",
        )
        manifest_path = write_manifest(manifest, cfg.paths.manifests_dir)
        con.execute(
            "INSERT OR REPLACE INTO run_manifest(run_id, slice_id, manifest_json, created_ts) VALUES(?,?,?,?);",
            (run_id, slice_id, Path(manifest_path).read_text(encoding="utf-8"), utc_now_iso()),
        )
        con.commit()
        con.close()

        logger.log("bootstrap.done", manifest_path=str(manifest_path))

        # opcional: commit/push
        if getattr(cfg.github_sync, "enabled", False) and getattr(cfg.github_sync, "commit", False) and not args.no_commit:
            logger.log("git.commit.start")
            sync_repo(
                repo_root=cfg.paths.repo_root,
                do_pull=False,
                do_commit=True,
                do_push=(getattr(cfg.github_sync, "push", False) and not args.no_push),
                commit_message=f"brickovery: M0 bootstrap (run_id={run_id}, slice_id={slice_id})",
                user_name=getattr(cfg.github_sync, "git_user_name", "github-actions[bot]"),
                user_email=getattr(cfg.github_sync, "git_user_email", "41898282+github-actions[bot]@users.noreply.github.com"),
                fail_safe_on_push_conflict=getattr(cfg.github_sync, "fail_safe_on_push_conflict", True),
            )
            logger.log("git.commit.done")

        return 0
    finally:
        lock.release()


# =========================
# M1: normalize-input
# =========================

@dataclass(frozen=True)
class _RawItem:
    bl_itemtype: str
    bl_part_id: str
    bl_color_id: Optional[int]
    qty: int
    condition: str


def _parse_search_xml(path: str) -> List[_RawItem]:
    """Parse BrickLink-style search.xml (robust).

    Supports:
      - full XML documents (any root, e.g. INVENTORY)
      - multiple <ITEM>...</ITEM> fragments concatenated (wrapped automatically)
      - tag case variations (ITEMID vs ItemId, etc.)
    """
    import re  # local import to keep patch self-contained

    txt = _read_text(path)
    txt = (txt or "").lstrip("\ufeff").strip()
    if not txt:
        return []

    # Remove an XML declaration if present (important when wrapping fragments)
    txt = re.sub(r"^\s*<\?xml[^>]*\?>\s*", "", txt, flags=re.IGNORECASE)

    def _local(tag: str) -> str:
        # Strip optional namespace: {ns}TAG -> TAG
        if "}" in tag:
            tag = tag.split("}", 1)[1]
        return tag

    def _iter_items(root_el):
        for el in root_el.iter():
            if _local(el.tag).lower() == "item":
                yield el

    def _parse_item_el(el):
        m: Dict[str, str] = {}
        for ch in list(el):
            k = _local(ch.tag).lower()
            v = (ch.text or "").strip()
            if k and v != "":
                m[k] = v
        # Some exports may carry values as attributes; include them too.
        for k, v in el.attrib.items():
            kk = _local(k).lower()
            vv = (v or "").strip()
            if kk and vv != "":
                m.setdefault(kk, vv)

        part_id = (m.get("itemid") or m.get("partid") or m.get("part_id") or m.get("itemno") or "").strip()
        itemtype = (m.get("itemtype") or m.get("item_type") or m.get("type") or "P").strip() or "P"
        color_raw = (m.get("color") or m.get("colorid") or m.get("colour") or "").strip()
        qty_raw = (m.get("qty") or m.get("minqty") or m.get("quantity") or "").strip()
        cond = (m.get("condition") or m.get("cond") or "N").strip().upper() or "N"

        if not part_id:
            raise ValueError("Found ITEM missing ITEMID/part id")

        try:
            qty = int(float(qty_raw)) if qty_raw else 0
        except Exception as e:
            raise ValueError(f"Invalid QTY='{qty_raw}' for ITEMID={part_id}") from e

        if qty <= 0:
            return None

        color_id: Optional[int] = None
        if color_raw:
            try:
                color_id = int(float(color_raw))
            except Exception as e:
                raise ValueError(f"Invalid COLOR='{color_raw}' for ITEMID={part_id}") from e

        if cond not in {"N", "U"}:
            raise ValueError(f"Invalid CONDITION='{cond}' for ITEMID={part_id} (expected N or U)")

        return _RawItem(itemtype, part_id, color_id, qty, cond)

    # Try: parse as complete XML; if that fails, wrap; if that fails, regex extract ITEM blocks.
    last_err: Optional[Exception] = None
    root = None
    for candidate in (txt, f"<ROOT>\n{txt}\n</ROOT>"):
        try:
            root = ET.fromstring(candidate)
            break
        except ET.ParseError as e:
            last_err = e
            root = None

    item_elems = []
    if root is not None:
        item_elems = list(_iter_items(root))
    else:
        blocks = re.findall(r"<\s*ITEM\b.*?<\s*/\s*ITEM\s*>", txt, flags=re.IGNORECASE | re.DOTALL)
        if not blocks:
            raise ValueError(f"search.xml is not parseable as XML and contains no <ITEM> blocks: {last_err}") from last_err
        for b in blocks:
            try:
                item_elems.append(ET.fromstring(b))
            except ET.ParseError as e:
                raise ValueError(f"Failed to parse ITEM block: {e}") from e

    out: List[_RawItem] = []
    for el in item_elems:
        parsed = _parse_item_el(el)
        if parsed is not None:
            out.append(parsed)

    return out


def _aggregate_items(raw: List[_RawItem]) -> List[_RawItem]:
    agg: Dict[Tuple[str, str, Optional[int], str], int] = {}
    for r in raw:
        k = (r.bl_itemtype, r.bl_part_id, r.bl_color_id, r.condition)
        agg[k] = agg.get(k, 0) + r.qty
    out = [_RawItem(k[0], k[1], k[2], q, k[3]) for k, q in agg.items()]
    out.sort(key=lambda x: (x.bl_itemtype, x.bl_part_id, x.bl_color_id or -1, x.condition))
    return out


def _ensure_m1_tables(con) -> None:
    # Estas tabelas devem existir via migrate(), mas garantimos por segurança
    con.execute("""
    CREATE TABLE IF NOT EXISTS mapping_overrides(
      bl_itemtype TEXT NOT NULL,
      bl_part_id TEXT NOT NULL,
      bl_color_id INTEGER,
      bo_boid TEXT,
      status TEXT NOT NULL,
      reason TEXT,
      created_ts TEXT NOT NULL,
      updated_ts TEXT NOT NULL,
      PRIMARY KEY (bl_itemtype, bl_part_id, bl_color_id)
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS up_mapping_mirror(
      bl_itemtype TEXT NOT NULL,
      bl_part_id TEXT NOT NULL,
      bl_color_id INTEGER,
      bo_boid TEXT,
      weight_mg INTEGER,
      source TEXT NOT NULL,
      row_hash TEXT NOT NULL,
      ingested_ts TEXT NOT NULL,
      PRIMARY KEY (bl_itemtype, bl_part_id, bl_color_id)
    );
    """)
    con.commit()


def _lookup_mapping(con, item: _RawItem) -> Tuple[Optional[str], Optional[int], str]:
    """
    Returns (boid, weight_mg, source) where source is 'override'|'upstream'|'missing'
    """
    boid_col = "bo_boid"
    # override first
    row = con.execute(
        """
        SELECT bo_boid, status
        FROM mapping_overrides
        WHERE bl_itemtype=? AND bl_part_id=? AND ( (bl_color_id IS NULL AND ? IS NULL) OR bl_color_id=? )
        """,
        (item.bl_itemtype, item.bl_part_id, item.bl_color_id, item.bl_color_id),
    ).fetchone()
    if row:
        boid, status = row[0], row[1]
        if status == "ok" and boid:
            # weight from upstream mirror if available
            wrow = con.execute(
                """
                SELECT weight_mg FROM up_mapping_mirror
                WHERE bl_itemtype=? AND bl_part_id=? AND ( (bl_color_id IS NULL AND ? IS NULL) OR bl_color_id=? )
                """,
                (item.bl_itemtype, item.bl_part_id, item.bl_color_id, item.bl_color_id),
            ).fetchone()
            weight_mg = int(wrow[0]) if (wrow and wrow[0] is not None) else None
            return str(boid), weight_mg, "override"
        # fill_later -> treat missing boid, but still try weight
        wrow = con.execute(
            """
            SELECT weight_mg FROM up_mapping_mirror
            WHERE bl_itemtype=? AND bl_part_id=? AND ( (bl_color_id IS NULL AND ? IS NULL) OR bl_color_id=? )
            """,
            (item.bl_itemtype, item.bl_part_id, item.bl_color_id, item.bl_color_id),
        ).fetchone()
        weight_mg = int(wrow[0]) if (wrow and wrow[0] is not None) else None
        return None, weight_mg, "missing"

    # upstream mirror
    row = con.execute(
        """
        SELECT bo_boid, weight_mg
        FROM up_mapping_mirror
        WHERE bl_itemtype=? AND bl_part_id=? AND ( (bl_color_id IS NULL AND ? IS NULL) OR bl_color_id=? )
        """,
        (item.bl_itemtype, item.bl_part_id, item.bl_color_id, item.bl_color_id),
    ).fetchone()
    if row:
        boid = row[0]
        weight_mg = int(row[1]) if row[1] is not None else None
        return (str(boid) if boid else None), weight_mg, ("upstream" if boid else "missing")

    return None, None, "missing"


def _ensure_fill_later(con, item: _RawItem, reason: str) -> None:
    now = utc_now_iso()
    con.execute(
        """
        INSERT OR IGNORE INTO mapping_overrides(bl_itemtype, bl_part_id, bl_color_id, bo_boid, status, reason, created_ts, updated_ts)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (item.bl_itemtype, item.bl_part_id, item.bl_color_id, None, "fill_later", reason, now, now),
    )
    # If already existed, refresh updated_ts/reason (idempotent)
    con.execute(
        """
        UPDATE mapping_overrides
        SET status='fill_later', reason=?, updated_ts=?
        WHERE bl_itemtype=? AND bl_part_id=? AND ( (bl_color_id IS NULL AND ? IS NULL) OR bl_color_id=? )
        """,
        (reason, now, item.bl_itemtype, item.bl_part_id, item.bl_color_id, item.bl_color_id),
    )


def cmd_normalize_input(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id

    logger = JsonlLogger(cfg.paths.logs_dir, run_id, slice_id)

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    try:
        lock.acquire(owner_run_id=run_id, owner_info={"slice_id": slice_id, "action": "normalize-input"}, wait_seconds=args.lock_wait_seconds)
    except RepoLockError as e:
        raise SystemExit(str(e))

    try:
        con = connect(cfg.paths.sqlite_db)
        migrate(con)
        _ensure_m1_tables(con)

        raw = _parse_search_xml(args.input)
        requested = _aggregate_items(raw)

        stats = {
            "raw_items": len(raw),
            "requested_items": len(requested),
            "mapped_ok": 0,
            "missing_boid": 0,
            "weight_ok": 0,
            "missing_weight": 0,
            "overrides_created": 0,
        }

        normalized: List[Dict[str, Any]] = []
        total_weight_mg = 0

        for it in requested:
            boid, weight_mg, source = _lookup_mapping(con, it)

            if boid:
                stats["mapped_ok"] += 1
            else:
                stats["missing_boid"] += 1
                _ensure_fill_later(con, it, reason="missing boid in upstream/override")
                stats["overrides_created"] += 1

            if weight_mg is not None:
                stats["weight_ok"] += 1
                total_weight_mg += weight_mg * it.qty
            else:
                stats["missing_weight"] += 1

            normalized.append(
                {
                    "bl_itemtype": it.bl_itemtype,
                    "bl_part_id": it.bl_part_id,
                    "bl_color_id": it.bl_color_id,
                    "qty": it.qty,
                    "condition": it.condition,
                    "boid": boid,
                    "weight_mg": weight_mg,
                    "mapping_source": source,
                }
            )

        up_state = _get_upstream_state(con)
        con.commit()
        con.close()

        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        normalized_path = out_dir / f"normalized_items.{run_id}.json"
        _write_json(str(normalized_path), normalized)

        input_sha = sha256_file(args.input)
        normalized_sha = sha256_file(str(normalized_path))
        normalized_hash = _hash_list(normalized)

        manifest = {
            "run_id": run_id,
            "slice_id": slice_id,
            "created_ts": _iso_now(),
            "input_path": args.input,
            "input_sha256": input_sha,
            "normalized_items_path": str(normalized_path),
            "normalized_items_sha256": normalized_sha,
            "normalized_items_hash": normalized_hash,
            "stats": stats,
            "total_weight_mg": total_weight_mg,
        }
        # adiciona upstream_state se existir (audit)
        manifest.update(up_state)

        manifest["manifest_hash"] = sha256_json(manifest)

        manifest_path = out_dir / "input_manifest.json"
        _write_json(str(manifest_path), manifest)

        logger.log("m1.normalize.done", **{k: v for k, v in manifest.items() if k != "stats"}, stats=stats)

        return 0
    finally:
        lock.release()


# =========================
# M2: refresh-cache (BrickLink HTML scraper)
# =========================

EU_ISO2 = {
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT",
    "LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE"
}
EXCLUDE_ISO2 = {"GB", "CH"}
PRICE_RE = re.compile(r"(?P<tilde>~)?\s*(?P<cur>EUR)\s*(?P<num>\d[\d\.,]*)", re.IGNORECASE)
BOX_RE = re.compile(r"box16([YN])\.png", re.IGNORECASE)
FLAG_RE = re.compile(r"/flagsS/([A-Z]{2})\.(gif|png)", re.IGNORECASE)
STORE_RE = re.compile(r"store\.asp\?", re.IGNORECASE)
IDITEM_JSON_RE = re.compile(r'"idItem"\s*:\s*"?([0-9]+)"?', re.IGNORECASE)

def _extract_id_item(html: str) -> Optional[int]:
    """Best-effort extraction of BrickLink internal idItem from catalog HTML.
    BrickLink varies markup; we try multiple patterns.
    """
    m = IDITEM_JSON_RE.search(html)
    if m:
        return int(m.group(1))
    patterns = [
        r"\bidItem\b\s*=\s*(\d+)",
        r"\bidItem\b\s*:\s*(\d+)",
        r"idItem=(\d+)",
        r"data-iditem=[\"\'](\d+)[\"\']",
        r"var\s+idItem\s*=\s*(\d+)",
    ]
    for pat in patterns:
        mm = re.search(pat, html, flags=re.IGNORECASE)
        if mm:
            return int(mm.group(1))
    return None


def _ensure_market_schema(con) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS market_metrics (
      platform TEXT NOT NULL,
      bl_itemtype TEXT NOT NULL,
      bl_part_id TEXT NOT NULL,
      bl_color_id INTEGER,
      condition TEXT NOT NULL,
      sold_6m_total_qty INTEGER,
      sold_6m_avg_price_eur TEXT,
      current_min_price_eur TEXT,
      current_min_price_is_estimate INTEGER,
      current_total_lots INTEGER,
      current_total_qty INTEGER,
      fetched_ts TEXT NOT NULL,
      snapshot_id TEXT NOT NULL,
      data_source TEXT NOT NULL,
      parser_version TEXT NOT NULL,
      PRIMARY KEY (platform, bl_itemtype, bl_part_id, bl_color_id, condition)
    );
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS market_offers (
      platform TEXT NOT NULL,
      snapshot_id TEXT NOT NULL,
      bl_itemtype TEXT NOT NULL,
      bl_part_id TEXT NOT NULL,
      bl_color_id INTEGER,
      condition TEXT NOT NULL,
      store_url TEXT,
      country_iso2 TEXT,
      ships_to_me INTEGER,
      qty INTEGER NOT NULL,
      price_eur TEXT,
      is_estimate INTEGER,
      fetched_ts TEXT NOT NULL,
      data_source TEXT NOT NULL,
      parser_version TEXT NOT NULL
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_market_offers_key ON market_offers(platform, bl_itemtype, bl_part_id, bl_color_id, condition);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_market_offers_snap ON market_offers(snapshot_id);")
    con.commit()


def _parse_price(text: str) -> Tuple[Optional[Decimal], bool]:
    text = " ".join(text.split())
    m = PRICE_RE.search(text)
    if not m:
        return None, False
    is_est = bool(m.group("tilde"))
    num = m.group("num")
    if "," in num and "." in num:
        num = num.replace(",", "")
    elif "," in num and "." not in num:
        num = num.replace(",", ".")
    return Decimal(num), is_est


def _offers_from_html(html: str) -> List[Dict[str, Any]]:
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except Exception as e:
        raise RuntimeError("beautifulsoup4 is required for scraping. Add 'beautifulsoup4' to requirements.txt") from e

    soup = BeautifulSoup(html, "html.parser")
    offers: List[Dict[str, Any]] = []

    for tr in soup.find_all("tr"):
        a = tr.find("a", href=STORE_RE)
        if not a:
            continue

        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        country = None
        flag_img = tr.find("img", src=FLAG_RE)
        if flag_img and flag_img.get("src"):
            mm = FLAG_RE.search(flag_img["src"])
            if mm:
                country = mm.group(1).upper()

        ships = None
        box_img = tr.find("img", src=BOX_RE)
        if box_img and box_img.get("src"):
            mm = BOX_RE.search(box_img["src"])
            if mm:
                ships = (mm.group(1).upper() == "Y")

        qty_txt = tds[1].get_text(strip=True)
        qty = int(qty_txt) if qty_txt.isdigit() else 0

        price = None
        is_est = False
        # procurar preço em qualquer td (tende a estar no fim)
        for td in reversed(tds):
            p, est = _parse_price(td.get_text(" ", strip=True))
            if p is not None:
                price, is_est = p, est
                break

        href = a.get("href") or ""
        if href and not href.lower().startswith("http"):
            href = "https://www.bricklink.com/" + href.lstrip("/")

        offers.append(
            {
                "store_url": href or None,
                "country_iso2": country,
                "ships_to_me": ships,
                "qty": qty,
                "price": price,
                "is_estimate": is_est,
            }
        )
    return offers


def _filter_eu_pt(offers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for o in offers:
        if o.get("ships_to_me") is not True:
            continue
        c = (o.get("country_iso2") or "").upper()
        if not c or c in EXCLUDE_ISO2 or c not in EU_ISO2:
            continue
        out.append(o)
    return out


def _compute_current_stats(offers: List[Dict[str, Any]]) -> Tuple[int, int, Optional[Decimal], Optional[bool]]:
    total_lots = len(offers)
    total_qty = sum(int(o.get("qty") or 0) for o in offers)
    min_price = None
    min_is_est = None
    for o in offers:
        p = o.get("price")
        if p is None:
            continue
        if min_price is None or p < min_price:
            min_price = p
            min_is_est = bool(o.get("is_estimate"))
    return total_lots, total_qty, min_price, min_is_est


def _parse_sold6m(html: str) -> Dict[str, Dict[str, Any]]:
    """
    Parse BrickLink Price Guide Summary "Past/Last 6 Months Sales" section.

    Returns:
      {"new": {"qty": int|None, "avg": Decimal|None},
       "used": {"qty": int|None, "avg": Decimal|None}}

    Notes:
      - This parser is intentionally heuristic and resilient to minor HTML/layout changes.
      - It expects the user session to have currency set to EUR; if not, parsing still occurs
        but the numeric value is returned as-is (caller may decide to accept/NULL it).
    """
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except Exception as e:
        raise RuntimeError("beautifulsoup4 is required for scraping. Add 'beautifulsoup4' to requirements.txt") from e

    soup = BeautifulSoup(html or "", "html.parser")
    out: Dict[str, Dict[str, Any]] = {"new": {"qty": None, "avg": None}, "used": {"qty": None, "avg": None}}

    def norm(s: str) -> str:
        return " ".join((s or "").strip().split()).lower()

    def parse_int(s: str) -> Optional[int]:
        s = (s or "").strip()
        m = re.search(r"\d[\d,\.]*", s)
        if not m:
            return None
        v = m.group(0)
        # thousands separators
        if v.count(",") > 0 and v.count(".") > 0:
            # assume ',' thousands
            v = v.replace(",", "")
        else:
            # if only commas, treat as thousands if looks like 1,234; else decimal comma unlikely for qty
            v = v.replace(",", "")
        try:
            return int(float(v))
        except Exception:
            return None

    def parse_decimal(s: str) -> Optional[Decimal]:
        s = (s or "").strip()
        # Strip currency symbols/words but keep digits + separators
        m = re.search(r"\d[\d,\.]*", s)
        if not m:
            return None
        v = m.group(0)
        # Normalize separators:
        # - "1,234.56" -> "1234.56"
        # - "1.234,56" -> "1234.56"
        if "," in v and "." in v:
            if v.rfind(",") > v.rfind("."):
                v = v.replace(".", "").replace(",", ".")
            else:
                v = v.replace(",", "")
        elif "," in v and "." not in v:
            # could be decimal comma or thousands; decide by trailing 2 digits
            if re.search(r",\d{2}$", v):
                v = v.replace(",", ".")
            else:
                v = v.replace(",", "")
        try:
            return Decimal(v)
        except Exception:
            return None

    # Locate anchor text for the sold section
    sold_anchor = None
    for node in soup.find_all(string=True):
        t = (node or "").strip()
        if re.search(r"(past|last)\s+6\s+months\s+sales", t, flags=re.IGNORECASE):
            sold_anchor = node
            break

    # If we can't find the section at all, fallback to very rough text scan.
    if sold_anchor is None:
        text = soup.get_text("\n")
        lines = [" ".join(l.split()) for l in text.splitlines() if l.strip()]
        in_sold = False
        for line in lines:
            if re.search(r"(past|last)\s+6\s+months\s+sales", line, flags=re.IGNORECASE):
                in_sold = True
                continue
            if in_sold and re.search(r"current\s+items\s+for\s+sale", line, flags=re.IGNORECASE):
                break
            # Sometimes appears like "New  123  EUR 0.05" etc.
            if in_sold and re.search(r"^(new|used)\b", line, flags=re.IGNORECASE):
                kind = "new" if line.lower().startswith("new") else "used"
                nums = re.findall(r"\d[\d,\.]*", line)
                if nums:
                    # Heuristic: first integer-like is qty, one of later is avg
                    qty = parse_int(nums[0])
                    avg = parse_decimal(nums[-1]) if len(nums) > 1 else None
                    if qty is not None:
                        out[kind]["qty"] = qty
                    if avg is not None:
                        out[kind]["avg"] = avg
        return out

    # Try to find the most relevant table near the sold section.
    # Strategy: from anchor node, walk up to a container and search for the first table that has
    # headers like "Avg Price"/"Average Price" and "Qty".
    container = sold_anchor.parent
    for _ in range(6):
        if container is None:
            break
        if getattr(container, "name", None) in ("table", "div", "section", "td"):
            break
        container = container.parent

    tables = []
    if container is not None:
        tables = container.find_all("table")
    if not tables:
        tables = soup.find_all("table")

    best = None
    best_score = -1
    for tbl in tables:
        hdrs = [norm(th.get_text(" ")) for th in tbl.find_all(["th"])]
        score = 0
        if any("avg" in h and "price" in h for h in hdrs) or any("average" in h and "price" in h for h in hdrs):
            score += 2
        if any("qty" in h for h in hdrs) or any("quantity" in h for h in hdrs):
            score += 2
        if any("new" in norm(tbl.get_text(" ")) for _ in [0]) and any("used" in norm(tbl.get_text(" ")) for _ in [0]):
            score += 1
        if score > best_score:
            best_score = score
            best = tbl

    if best is None:
        return out

    # Build header index mapping (best effort)
    header_cells = best.find_all("th")
    headers = [norm(th.get_text(" ")) for th in header_cells]
    idx_qty = None
    idx_avg = None

    # Try row-wise headers if present
    # Many BL tables have headers in first row; if so, use that.
    first_tr = best.find("tr")
    if first_tr:
        ths = first_tr.find_all(["th", "td"])
        # if first row looks like headers (contains avg/qty words)
        maybe_headers = [norm(x.get_text(" ")) for x in ths]
        if any("avg" in h and "price" in h for h in maybe_headers) or any("qty" in h for h in maybe_headers):
            headers = maybe_headers

    for i, h in enumerate(headers):
        if idx_qty is None and (("total qty" in h) or ("qty sold" in h) or ("quantity sold" in h) or (h == "qty") or ("qty" in h and "avg" not in h)):
            idx_qty = i
        if idx_avg is None and (("avg price" in h) or ("average price" in h) or ("avg" in h and "price" in h)):
            idx_avg = i

    # Fallback indices if header map not found (common pattern: Type | Total Qty | Total Sales | Avg Price | Min | Max)
    # In that pattern: qty=1, avg=3
    if idx_qty is None:
        idx_qty = 1
    if idx_avg is None:
        idx_avg = 3

    # Parse rows
    for tr in best.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) < 2:
            continue
        first = norm(tds[0].get_text(" "))
        kind = None
        if first.startswith("new"):
            kind = "new"
        elif first.startswith("used"):
            kind = "used"
        else:
            continue

        qty = None
        avg = None
        if idx_qty < len(tds):
            qty = parse_int(tds[idx_qty].get_text(" "))
        if idx_avg < len(tds):
            avg = parse_decimal(tds[idx_avg].get_text(" "))

        # If still missing, attempt heuristic from all numeric cells in row
        if qty is None or avg is None:
            nums = [c.get_text(" ") for c in tds[1:]]
            parsed_nums = [parse_decimal(x) for x in nums if parse_decimal(x) is not None]
            parsed_ints = [parse_int(x) for x in nums if parse_int(x) is not None]
            if qty is None and parsed_ints:
                qty = parsed_ints[0]
            if avg is None and parsed_nums:
                # Heuristic: pick a "small-ish" value that looks like avg (often not max/min)
                # Prefer the second numeric if there are >=2, else last.
                avg = parsed_nums[1] if len(parsed_nums) >= 2 else parsed_nums[-1]

        if qty is not None:
            out[kind]["qty"] = qty
        if avg is not None:
            out[kind]["avg"] = avg

    return out



def _fetch(session, url: str, params: Dict[str, Any], headers: Dict[str, str], timeout_s: int, max_tries: int, delay_s: float) -> str:
    if delay_s > 0:
        time.sleep(delay_s)
    last = None
    for attempt in range(1, max_tries + 1):
        try:
            r = session.get(url, params=params, headers=headers, timeout=timeout_s)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(12.0, 2.0 ** attempt))
                continue
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(min(12.0, 2.0 ** attempt))
    detail = ""
    if last is not None:
        resp = getattr(last, "response", None)
        if resp is not None:
            detail += f" status={getattr(resp, 'status_code', None)}"
        detail += f" last_error={type(last).__name__}: {last}"
    raise RuntimeError(f"Failed to fetch {url} after {max_tries} tries.{detail}") from last


def cmd_refresh_cache(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id
    snapshot_id = args.snapshot_id or run_id
    ttl_hours = int(args.ttl_hours)

    logger = JsonlLogger(cfg.paths.logs_dir, run_id, slice_id)

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    try:
        lock.acquire(owner_run_id=run_id, owner_info={"slice_id": slice_id, "action": "refresh-cache"}, wait_seconds=args.lock_wait_seconds)
    except RepoLockError as e:
        raise SystemExit(str(e))

    try:
        con = connect(cfg.paths.sqlite_db)
        migrate(con)
        _ensure_market_schema(con)

        norm_path = args.normalized_items
        if not norm_path:
            p = Path("outputs/inputs")
            cands = sorted(p.glob("normalized_items.*.json"))
            if not cands:
                raise RuntimeError("No normalized_items.*.json found. Run normalize-input first.")
            norm_path = str(cands[-1])

        items = json.loads(_read_text(norm_path))
        cookie = os.getenv(args.cookie_env_var, None)
        if not cookie:
            raise RuntimeError(f"{args.cookie_env_var} is required (BrickLink logged-in cookie) to enforce ships-to-PT filter.")

        import requests  # lazy

        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.bricklink.com/",
            "Cookie": cookie,
        }

        url_catalog = "https://www.bricklink.com/v2/catalog/catalogitem.page"
        url_offers = "https://www.bricklink.com/v2/catalog/catalogitem_pgtab.page"
        url_sold = "https://www.bricklink.com/priceGuideSummary.asp"

        fetched_ts = _iso_now()
        data_source = "scrape"
        parser_version = "bl_html_v1"

        def _last_ts(itype: str, pid: str, cid: int, cond: str) -> Optional[str]:
            row = con.execute(
                """
                SELECT fetched_ts FROM market_metrics
                WHERE platform='BL' AND bl_itemtype=? AND bl_part_id=? AND bl_color_id=? AND condition=?
                """,
                (itype, pid, cid, cond),
            ).fetchone()
            return row[0] if row else None

        def _fresh(ts: Optional[str]) -> bool:
            if not ts:
                return False
            try:
                dt = _parse_iso(ts)
                return (datetime.now(timezone.utc) - dt) <= timedelta(hours=ttl_hours)
            except Exception:
                return False

        refreshed = 0
        skipped = 0
        errors = 0

        for it in items:
            itype = it["bl_itemtype"]
            pid = it["bl_part_id"]
            cid = it.get("bl_color_id")
            if cid is None:
                skipped += 1
                continue
            cid = int(cid)

            if not args.force and _fresh(_last_ts(itype, pid, cid, "N")) and _fresh(_last_ts(itype, pid, cid, "U")):
                skipped += 1
                continue

            try:
                html_cat = _fetch(session, url_catalog, params={itype.upper(): pid}, headers=headers, timeout_s=args.timeout_s, max_tries=args.max_tries, delay_s=0.0)
                id_item = _extract_id_item(html_cat)
                if id_item is None:
                    head = (html_cat or "")[:200].replace("\n", " ").replace("\r", " ")
                    raise RuntimeError(f"Could not resolve idItem for {itype}:{pid} (html_head={head!r})")

                html_sold = _fetch(
                    session,
                    url_sold,
                    params={"a": itype.upper(), "itemID": str(id_item), "colorID": str(cid), "vatInc": "Y", "vcID": "2"},
                    headers=headers,
                    timeout_s=args.timeout_s,
                    max_tries=args.max_tries,
                    delay_s=0.0,
                )
                sold = _parse_sold6m(html_sold)

                html_n = _fetch(session, url_offers, params={"idItem": str(id_item), "idColor": str(cid), "st": "1", "prec": "3", "showflag": "1", "showbulk": "1", "currency": "2"}, headers=headers, timeout_s=args.timeout_s, max_tries=args.max_tries, delay_s=args.delay_s)
                offers_n = _filter_eu_pt(_offers_from_html(html_n))
                lots_n, qty_n, minp_n, minp_est_n = _compute_current_stats(offers_n)

                html_u = _fetch(session, url_offers, params={"idItem": str(id_item), "idColor": str(cid), "st": "2", "prec": "3", "showflag": "1", "showbulk": "1", "currency": "2"}, headers=headers, timeout_s=args.timeout_s, max_tries=args.max_tries, delay_s=args.delay_s)
                offers_u = _filter_eu_pt(_offers_from_html(html_u))
                lots_u, qty_u, minp_u, minp_est_u = _compute_current_stats(offers_u)

                def upsert(cond: str, sold_kind: str, lots: int, qty: int, minp: Optional[Decimal], minp_est: Optional[bool], offers: List[Dict[str, Any]]):
                    con.execute(
                        """
                        INSERT OR REPLACE INTO market_metrics(
                          platform, bl_itemtype, bl_part_id, bl_color_id, condition,
                          sold_6m_total_qty, sold_6m_avg_price_eur,
                          current_min_price_eur, current_min_price_is_estimate,
                          current_total_lots, current_total_qty,
                          fetched_ts, snapshot_id, data_source, parser_version
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            "BL", itype, pid, cid, cond,
                            sold[sold_kind]["qty"], str(sold[sold_kind]["avg"]) if sold[sold_kind]["avg"] is not None else None,
                            str(minp) if minp is not None else None,
                            1 if minp_est else 0 if minp_est is not None else None,
                            lots, qty,
                            fetched_ts, snapshot_id, data_source, parser_version,
                        ),
                    )
                    con.execute(
                        "DELETE FROM market_offers WHERE platform='BL' AND bl_itemtype=? AND bl_part_id=? AND bl_color_id=? AND condition=?;",
                        (itype, pid, cid, cond),
                    )
                    for o in offers:
                        con.execute(
                            """
                            INSERT INTO market_offers(
                              platform, snapshot_id, bl_itemtype, bl_part_id, bl_color_id, condition,
                              store_url, country_iso2, ships_to_me, qty, price_eur, is_estimate,
                              fetched_ts, data_source, parser_version
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                "BL", snapshot_id, itype, pid, cid, cond,
                                o.get("store_url"),
                                o.get("country_iso2"),
                                1 if o.get("ships_to_me") else 0 if o.get("ships_to_me") is not None else None,
                                int(o.get("qty") or 0),
                                str(o.get("price")) if o.get("price") is not None else None,
                                1 if o.get("is_estimate") else 0,
                                fetched_ts, data_source, parser_version,
                            ),
                        )

                upsert("N", "new", lots_n, qty_n, minp_n, minp_est_n, offers_n)
                upsert("U", "used", lots_u, qty_u, minp_u, minp_est_u, offers_u)

                refreshed += 1
            except Exception as e:
                errors += 1
                logger.log("m2.error", bl_itemtype=itype, bl_part_id=pid, bl_color_id=cid, error=str(e))

        con.commit()
        con.close()

        out_dir = Path("outputs/market")
        out_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "run_id": run_id,
            "slice_id": slice_id,
            "snapshot_id": snapshot_id,
            "created_ts": fetched_ts,
            "ttl_hours": ttl_hours,
            "refreshed_items": refreshed,
            "skipped_items": skipped,
            "errors": errors,
            "normalized_items_path": norm_path,
            "cookie_env_var": args.cookie_env_var,
            "parser_version": parser_version,
            "data_source": data_source,
        }
        _write_json(str(out_dir / f"refresh_report.{run_id}.json"), report)
        logger.log("m2.done", **report)

        return 0
    finally:
        lock.release()


# =========================
# CLI wiring
# =========================

# =========================
# M3: solve (greedy seed scenarios)
# =========================

def _latest_glob(paths: List[str]) -> Optional[str]:
    cand: List[str] = []
    for pat in paths:
        cand.extend(glob.glob(pat))
    if not cand:
        return None
    cand.sort()
    return cand[-1]


def _load_normalized_items(path: Optional[str]) -> Tuple[str, List[Dict[str, Any]]]:
    if path:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"normalized-items not found: {path}")
        data = json.loads(p.read_text(encoding="utf-8"))
        return str(p), data

    latest = _latest_glob(["outputs/inputs/normalized_items.*.json"])
    if not latest:
        raise FileNotFoundError("No normalized_items.*.json found in outputs/inputs/. Run M1 first.")
    p = Path(latest)
    data = json.loads(p.read_text(encoding="utf-8"))
    return str(p), data


def _dec(x: Optional[str]) -> Optional[Decimal]:
    """Parse a money/number string into Decimal.

    Accepts formats like:
      - '0.12', '0,12'
      - 'EUR 0.12', '~EUR 0,12', '€0,12'
      - '1,234.56' or '1.234,56' (best-effort)
    """
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None

    # Normalize common currency/estimate markers
    s = s.replace("\u00a0", " ")
    s = s.replace("~", "")
    s = s.replace("EUR", "")
    s = s.replace("€", "")
    s = s.strip()
    s = s.replace(" ", "")

    # Extract a plausible numeric token
    m = re.search(r"-?\d[\d.,]*", s)
    if not m:
        return None
    num = m.group(0)

    # Heuristic for mixed separators
    if "," in num and "." in num:
        last_comma = num.rfind(",")
        last_dot = num.rfind(".")
        if last_comma > last_dot:
            # comma is decimal separator
            num = num.replace(".", "")
            num = num.replace(",", ".")
        else:
            # dot is decimal separator
            num = num.replace(",", "")
    elif "," in num and "." not in num:
        num = num.replace(",", ".")

    try:
        return Decimal(num)
    except Exception:
        return None


def _shipping_bands(con, service_level: str, dest_country: str = "PT") -> List[Tuple[int, int, Decimal]]:
    rows = con.execute(
        "SELECT weight_min_mg, weight_max_mg, price_eur FROM shipping_band WHERE service_level=? AND dest_country=? ORDER BY weight_min_mg ASC;",
        (service_level, dest_country),
    ).fetchall()
    out: List[Tuple[int, int, Decimal]] = []
    for r in rows:
        out.append((int(r[0]), int(r[1]), _dec(str(r[2])) or Decimal("0")))
    return out


def _shipping_cost_from_bands(bands: List[Tuple[int, int, Decimal]], weight_mg: int) -> Optional[Decimal]:
    for mn, mx, price in bands:
        if mn <= weight_mg <= mx:
            return price
    return None


def _ensure_solver_tables(con) -> None:
    con.execute(
        '''
        CREATE TABLE IF NOT EXISTS solver_solutions(
          run_id TEXT NOT NULL,
          scenario TEXT NOT NULL,
          created_ts TEXT NOT NULL,
          snapshot_id TEXT NOT NULL,
          normalized_items_path TEXT NOT NULL,
          total_score_eur TEXT,
          total_parts_cost_eur TEXT,
          total_shipping_cost_eur TEXT,
          n_stores_used INTEGER,
          missing_items INTEGER,
          payload_json TEXT NOT NULL,
          PRIMARY KEY (run_id, scenario)
        );
        '''
    )
    con.commit()


def _select_snapshot_id(con) -> str:
    # Prefer latest snapshot present in market_metrics; fallback to market_offers.
    row = con.execute("SELECT snapshot_id, MAX(fetched_ts) FROM market_metrics;").fetchone()
    if row and row[0]:
        return str(row[0])
    row = con.execute("SELECT snapshot_id, MAX(fetched_ts) FROM market_offers;").fetchone()
    if row and row[0]:
        return str(row[0])
    raise RuntimeError("No market data found. Run M2 refresh-cache first.")


def cmd_solve(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id

    logger = JsonlLogger(cfg.paths.logs_dir, run_id, slice_id)

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    try:
        lock.acquire(owner_run_id=run_id, owner_info={"slice_id": slice_id, "action": "solve"}, wait_seconds=args.lock_wait_seconds)
    except RepoLockError as e:
        raise SystemExit(str(e))

    try:
        # optional repo sync
        if getattr(cfg.github_sync, "enabled", True) and not args.no_pull:
            sync_repo(
                repo_root=cfg.paths.repo_root,
                pull=getattr(cfg.github_sync, "pull", True),
                commit=False,
                push=False,
                git_user_name=getattr(cfg.github_sync, "git_user_name", "github-actions[bot]"),
                git_user_email=getattr(cfg.github_sync, "git_user_email", "41898282+github-actions[bot]@users.noreply.github.com"),
                logger=logger,
            )

        norm_path, items = _load_normalized_items(args.normalized_items)

        con = connect(cfg.paths.sqlite_db)
        migrate(con)
        _ensure_market_schema(con)
        _ensure_solver_tables(con)

        snapshot_id = args.snapshot_id or _select_snapshot_id(con)

        # load shipping bands (for both services)
        bands_normal = _shipping_bands(con, "normal", "PT")
        bands_reg = _shipping_bands(con, "registered", "PT")

        if not bands_normal or not bands_reg:
            raise RuntimeError("Shipping bands missing in DB. Run M0 bootstrap to ingest shipping tables.")

        # scenario defaults
        penalty_store_default = Decimal(str(getattr(cfg.raw.get("solver", {}), "penalty_store", 3))) if isinstance(cfg.raw, dict) else Decimal("3")
        # robust defaults even if cfg.raw not populated
        penalty_store_default = Decimal(str(getattr(args, "penalty_store", None) or 3))
        base_excess_penalty_default = Decimal(str(getattr(args, "base_excess_penalty", None) or 1))

        scenarios = [
            {"name": "min_total_normal", "penalty_store": Decimal(str(args.penalty_store or 3)), "base_excess_penalty": Decimal(str(args.base_excess_penalty or 1)), "ship_service": "normal"},
            {"name": "min_stores_normal", "penalty_store": Decimal(str(args.penalty_store_high or 10)), "base_excess_penalty": Decimal(str(args.base_excess_penalty or 1)), "ship_service": "normal"},
            {"name": "base_strict_normal", "penalty_store": Decimal(str(args.penalty_store or 3)), "base_excess_penalty": Decimal(str(args.base_excess_penalty_strict or 5)), "ship_service": "normal"},
            {"name": "min_total_registered", "penalty_store": Decimal(str(args.penalty_store or 3)), "base_excess_penalty": Decimal(str(args.base_excess_penalty or 1)), "ship_service": "registered"},
            {"name": "min_stores_registered", "penalty_store": Decimal(str(args.penalty_store_high or 10)), "base_excess_penalty": Decimal(str(args.base_excess_penalty or 1)), "ship_service": "registered"},
        ]

        # Build offer pool for all items (across platforms if present)
        # We treat each market_offers row as a "lot".
        def fetch_offers_for_item(it: Dict[str, Any]) -> List[Dict[str, Any]]:
            rows = con.execute(
                '''
                SELECT rowid, platform, store_url, country_iso2, ships_to_me, qty, price_eur, is_estimate, fetched_ts, data_source, parser_version
                FROM market_offers
                WHERE snapshot_id=?
                  AND bl_itemtype=?
                  AND bl_part_id=?
                  AND ( (bl_color_id IS NULL AND ? IS NULL) OR bl_color_id=? )
                  AND condition=?
                  AND price_eur IS NOT NULL
                ''',
                (snapshot_id, it["bl_itemtype"], it["bl_part_id"], it.get("bl_color_id"), it.get("bl_color_id"), it["condition"]),
            ).fetchall()

            offers: List[Dict[str, Any]] = []
            for r in rows:
                platform = r[1]
                country = (r[3] or "").upper()
                ships = int(r[4] or 0)
                if ships != 1:
                    continue
                if country and country not in EU_ISO2:
                    continue
                offers.append({
                    "lot_id": int(r[0]),
                    "platform": platform,
                    "store_url": r[2] or "",
                    "country_iso2": country,
                    "ships_to_me": ships,
                    "qty": int(r[5] or 0),
                    "price_eur": str(r[6]),
                    "is_estimate": int(r[7] or 0),
                    "fetched_ts": r[8],
                    "data_source": r[9],
                    "parser_version": r[10],
                })
            return offers

        def get_base_value(it: Dict[str, Any]) -> Optional[Decimal]:
            """Return the latest non-null BASE_VALUE (avg sold 6m) for this item+condition.

            Market metrics may contain multiple rows over time; we must avoid accidentally
            picking an older row where sold_6m_avg_price_eur is NULL.
            """
            # Discover a stable ordering column if present; otherwise fall back to rowid.
            try:
                cols = {r[1] for r in con.execute("PRAGMA table_info(market_metrics)").fetchall()}
            except Exception:
                cols = set()

            order_col = None
            for c in ("updated_ts", "ingested_ts", "fetched_ts", "created_ts", "snapshot_id"):
                if c in cols:
                    order_col = c
                    break

            order_sql = f"ORDER BY {order_col} DESC" if order_col else "ORDER BY rowid DESC"

            sql = f'''
                SELECT sold_6m_avg_price_eur
                FROM market_metrics
                WHERE platform='BL'
                  AND bl_itemtype=?
                  AND bl_part_id=?
                  AND ( (bl_color_id IS NULL AND ? IS NULL) OR bl_color_id=? )
                  AND condition=?
                  AND sold_6m_avg_price_eur IS NOT NULL
                {order_sql}
                LIMIT 1
            '''

            row = con.execute(
                sql,
                (it["bl_itemtype"], it["bl_part_id"], it.get("bl_color_id"), it.get("bl_color_id"), it["condition"]),
            ).fetchone()

            if not row:
                return None

            try:
                return _dec(str(row[0]))
            except Exception:
                return None

        # order items by operational rarity (fewest eligible stores)
        item_meta: List[Tuple[int, str, Dict[str, Any]]] = []
        for it in items:
            offers = fetch_offers_for_item(it)
            store_count = len({f'{o["platform"]}|{o["store_url"]}' for o in offers})
            key = f'{it["bl_itemtype"]}:{it["bl_part_id"]}:{it.get("bl_color_id")}:{it["condition"]}'
            item_meta.append((store_count if store_count > 0 else 10**9, key, it))
        item_meta.sort(key=lambda t: (t[0], t[1]))
        ordered_items = [t[2] for t in item_meta]

        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        created_ts = _iso_now()

        all_results: Dict[str, Any] = {
            "run_id": run_id,
            "slice_id": slice_id,
            "created_ts": created_ts,
            "snapshot_id": snapshot_id,
            "normalized_items_path": norm_path,
            "scenarios": [],
        }

        def run_scenario(scn: Dict[str, Any]) -> Dict[str, Any]:
            penalty_store = Decimal(str(scn["penalty_store"]))
            base_excess_penalty = Decimal(str(scn["base_excess_penalty"]))
            ship_service = scn["ship_service"]
            bands_score = bands_normal if ship_service == "normal" else bands_reg

            # store state
            store_weight: Dict[str, int] = {}
            store_used: Dict[str, bool] = {}
            store_allocs: Dict[str, List[Dict[str, Any]]] = {}

            totals = {
                "parts_cost": Decimal("0"),
                "base_excess_penalty": Decimal("0"),
                "estimates_penalty": Decimal("0"),
                "penalty_store_total": Decimal("0"),
            }
            missing: List[Dict[str, Any]] = []

            # offer remaining per item
            offers_cache: Dict[str, List[Dict[str, Any]]] = {}

            for it in ordered_items:
                key = f'{it["bl_itemtype"]}|{it["bl_part_id"]}|{it.get("bl_color_id")}|{it["condition"]}'
                if key not in offers_cache:
                    offers_cache[key] = fetch_offers_for_item(it)
                offers = offers_cache[key]

                need = int(it["qty"])
                if need <= 0:
                    continue

                base_value = get_base_value(it)
                weight_mg = it.get("weight_mg")
                weight_mg = int(weight_mg) if weight_mg is not None else None

                # local remaining copy
                rem = need
                # track remaining quantities per offer
                rem_qty = {o["lot_id"]: int(o["qty"]) for o in offers}

                while rem > 0:
                    best = None
                    best_unit = None
                    best_take = 0

                    for o in offers:
                        avail = rem_qty.get(o["lot_id"], 0)
                        if avail <= 0:
                            continue
                        take = min(avail, rem)

                        price = _dec(o["price_eur"])
                        if price is None:
                            continue

                        excess = Decimal("0")
                        if base_value is not None and price > base_value:
                            excess = (price - base_value) * Decimal(take) * base_excess_penalty

                        est_pen = Decimal("0")
                        if int(o.get("is_estimate") or 0) == 1:
                            # small deterministic penalty to prefer confirmed where equal
                            est_pen = Decimal(take) * Decimal("0.01")

                        store_key = f'{o["platform"]}|{o["store_url"]}'
                        was_used = store_used.get(store_key, False)

                        activation = Decimal("0")
                        if not was_used:
                            activation = penalty_store

                        # shipping delta for scoring service
                        old_w = store_weight.get(store_key, 0)
                        add_w = (weight_mg or 0) * take
                        new_w = old_w + add_w

                        old_ship = _shipping_cost_from_bands(bands_score, old_w) if old_w > 0 else Decimal("0")
                        new_ship = _shipping_cost_from_bands(bands_score, new_w) if new_w > 0 else Decimal("0")

                        if old_ship is None or new_ship is None:
                            ship_delta = Decimal("0")
                        else:
                            ship_delta = (new_ship - old_ship)

                        parts = price * Decimal(take)
                        total_marg = parts + excess + est_pen + activation + ship_delta
                        unit = total_marg / Decimal(take)

                        tiebreak = (unit, price, int(o.get("is_estimate") or 0), store_key, o["lot_id"])
                        if best is None or tiebreak < best_unit:
                            best = o
                            best_unit = tiebreak
                            best_take = take

                    if best is None:
                        break

                    # commit allocation
                    lot_id = best["lot_id"]
                    take = best_take
                    rem_qty[lot_id] -= take
                    rem -= take

                    price = _dec(best["price_eur"]) or Decimal("0")
                    store_key = f'{best["platform"]}|{best["store_url"]}'
                    if not store_used.get(store_key, False):
                        store_used[store_key] = True
                        totals["penalty_store_total"] += penalty_store

                    old_w = store_weight.get(store_key, 0)
                    add_w = (weight_mg or 0) * take
                    store_weight[store_key] = old_w + add_w

                    # cost accumulators
                    totals["parts_cost"] += price * Decimal(take)
                    if base_value is not None and price > base_value:
                        totals["base_excess_penalty"] += (price - base_value) * Decimal(take) * base_excess_penalty
                    if int(best.get("is_estimate") or 0) == 1:
                        totals["estimates_penalty"] += Decimal(take) * Decimal("0.01")

                    alloc = {
                        "bl_itemtype": it["bl_itemtype"],
                        "bl_part_id": it["bl_part_id"],
                        "bl_color_id": it.get("bl_color_id"),
                        "condition": it["condition"],
                        "qty": take,
                        "unit_price_eur": str(price),
                        "base_value_eur": str(base_value) if base_value is not None else None,
                        "platform": best["platform"],
                        "store_url": best["store_url"],
                        "lot_id": lot_id,
                        "is_estimate": int(best.get("is_estimate") or 0),
                        "country_iso2": best.get("country_iso2"),
                        "ships_to_me": int(best.get("ships_to_me") or 0),
                        "fetched_ts": best.get("fetched_ts"),
                        "data_source": best.get("data_source"),
                        "parser_version": best.get("parser_version"),
                    }
                    store_allocs.setdefault(store_key, []).append(alloc)

                if rem > 0:
                    missing.append({
                        "bl_itemtype": it["bl_itemtype"],
                        "bl_part_id": it["bl_part_id"],
                        "bl_color_id": it.get("bl_color_id"),
                        "condition": it["condition"],
                        "qty_missing": rem,
                        "reason": "insufficient eligible offers",
                    })

            # compute shipping totals (both)
            shipping_normal = Decimal("0")
            shipping_registered = Decimal("0")
            shipping_unknown = False

            store_summaries: List[Dict[str, Any]] = []
            for sk, w in store_weight.items():
                c_norm = _shipping_cost_from_bands(bands_normal, w) if w > 0 else Decimal("0")
                c_reg = _shipping_cost_from_bands(bands_reg, w) if w > 0 else Decimal("0")
                if c_norm is None or c_reg is None:
                    shipping_unknown = True
                    c_norm = c_norm or Decimal("0")
                    c_reg = c_reg or Decimal("0")
                shipping_normal += c_norm
                shipping_registered += c_reg
                store_summaries.append({
                    "store_key": sk,
                    "weight_mg": w,
                    "shipping_normal_eur": str(c_norm),
                    "shipping_registered_eur": str(c_reg),
                    "n_allocs": len(store_allocs.get(sk, [])),
                })
            store_summaries.sort(key=lambda d: (d["store_key"]))

            # choose scoring shipping based on scenario
            shipping_score = shipping_normal if ship_service == "normal" else shipping_registered

            total_score = totals["parts_cost"] + shipping_score + totals["penalty_store_total"] + totals["base_excess_penalty"] + totals["estimates_penalty"]
            return {
                "scenario": scn["name"],
                "ship_service_for_score": ship_service,
                "params": {
                    "penalty_store": str(penalty_store),
                    "base_excess_penalty": str(base_excess_penalty),
                },
                "snapshot_id": snapshot_id,
                "n_stores_used": len(store_used),
                "missing_items": missing,
                "shipping_unknown": shipping_unknown,
                "costs": {
                    "parts_cost_eur": str(totals["parts_cost"]),
                    "base_excess_penalty_eur": str(totals["base_excess_penalty"]),
                    "estimates_penalty_eur": str(totals["estimates_penalty"]),
                    "penalty_store_total_eur": str(totals["penalty_store_total"]),
                    "shipping_normal_eur": str(shipping_normal),
                    "shipping_registered_eur": str(shipping_registered),
                    "shipping_score_eur": str(shipping_score),
                    "total_score_eur": str(total_score),
                },
                "stores": store_summaries,
                "allocations": store_allocs,
            }

        for scn in scenarios:
            res = run_scenario(scn)
            all_results["scenarios"].append(res)

            # persist to DB
            con.execute(
                "INSERT OR REPLACE INTO solver_solutions(run_id, scenario, created_ts, snapshot_id, normalized_items_path, total_score_eur, total_parts_cost_eur, total_shipping_cost_eur, n_stores_used, missing_items, payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?);",
                (
                    run_id,
                    res["scenario"],
                    created_ts,
                    snapshot_id,
                    norm_path,
                    res["costs"]["total_score_eur"],
                    res["costs"]["parts_cost_eur"],
                    res["costs"]["shipping_score_eur"],
                    int(res["n_stores_used"]),
                    int(len(res["missing_items"])),
                    json.dumps(res, ensure_ascii=False),
                ),
            )

        con.commit()
        con.close()

        out_path = out_dir / f"seed_solutions.{run_id}.json"
        _write_json(str(out_path), all_results)

        # light checkpoint (idempotent)
        ck_dir = out_dir / "checkpoints"
        ck_dir.mkdir(parents=True, exist_ok=True)
        _write_json(str(ck_dir / f"seed.{run_id}.json"), {"run_id": run_id, "created_ts": created_ts, "snapshot_id": snapshot_id})

        logger.log("m3.solve.done", snapshot_id=snapshot_id, normalized_items_path=norm_path, scenarios=len(scenarios), output=str(out_path))

        # commit/push if enabled
        if getattr(cfg.github_sync, "enabled", True) and not args.no_commit:
            sync_repo(
                repo_root=cfg.paths.repo_root,
                pull=False,
                commit=not args.no_commit,
                push=not args.no_push,
                git_user_name=getattr(cfg.github_sync, "git_user_name", "github-actions[bot]"),
                git_user_email=getattr(cfg.github_sync, "git_user_email", "41898282+github-actions[bot]@users.noreply.github.com"),
                logger=logger,
                commit_message=f"brickovery: solve seeds (run_id={run_id}, slice_id={slice_id}, snapshot_id={snapshot_id})",
            )

        return 0
    finally:
        lock.release()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="brickovery")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bootstrap", help="M0: ingest upstream + shipping + rarity + write run manifest")
    b.add_argument("--config", required=True)
    b.add_argument("--run-id", default=None)
    b.add_argument("--slice-id", default="0")
    b.add_argument("--upstream-checkout-path", default="upstream/amauri-repo")
    b.add_argument("--no-pull", action="store_true")
    b.add_argument("--no-commit", action="store_true")
    b.add_argument("--no-push", action="store_true")
    b.add_argument("--lock-ttl-seconds", type=int, default=3600)
    b.add_argument("--lock-wait-seconds", type=int, default=0)
    b.set_defaults(func=cmd_bootstrap)

    n = sub.add_parser("normalize-input", help="M1: parse search.xml + normalize + mapping overrides + input_manifest")
    n.add_argument("--config", required=True)
    n.add_argument("--input", required=True)
    n.add_argument("--output-dir", required=True)
    n.add_argument("--run-id", default=None)
    n.add_argument("--slice-id", default="0")
    n.add_argument("--lock-ttl-seconds", type=int, default=3600)
    n.add_argument("--lock-wait-seconds", type=int, default=0)
    n.set_defaults(func=cmd_normalize_input)

    m2 = sub.add_parser("refresh-cache", help="M2: refresh market cache + metrics (TTL enforced, BrickLink HTML)")
    m2.add_argument("--config", required=True)
    m2.add_argument("--normalized-items", default=None)
    m2.add_argument("--run-id", default=None)
    m2.add_argument("--slice-id", default="0")
    m2.add_argument("--snapshot-id", default=None)
    m2.add_argument("--ttl-hours", default="6")
    m2.add_argument("--force", action="store_true")
    m2.add_argument("--cookie-env-var", default="BRICKLINK_COOKIE")
    m2.add_argument("--delay-s", type=float, default=0.9)
    m2.add_argument("--timeout-s", type=int, default=25)
    m2.add_argument("--max-tries", type=int, default=5)
    m2.add_argument("--lock-ttl-seconds", type=int, default=3600)
    m2.add_argument("--lock-wait-seconds", type=int, default=0)
    m2.set_defaults(func=cmd_refresh_cache)


    s = sub.add_parser("solve", help="M3: greedy seed solver (3-5 scenarios) using market_offers + shipping bands")
    s.add_argument("--config", required=True)
    s.add_argument("--normalized-items", default=None)
    s.add_argument("--output-dir", default="outputs/solver")
    s.add_argument("--snapshot-id", default=None)
    s.add_argument("--run-id", default=None)
    s.add_argument("--slice-id", default="0")
    s.add_argument("--penalty-store", type=int, default=3)
    s.add_argument("--penalty-store-high", type=int, default=10)
    s.add_argument("--base-excess-penalty", type=int, default=1)
    s.add_argument("--base-excess-penalty-strict", type=int, default=5)
    s.add_argument("--no-pull", action="store_true")
    s.add_argument("--no-commit", action="store_true")
    s.add_argument("--no-push", action="store_true")
    s.add_argument("--lock-ttl-seconds", type=int, default=3600)
    s.add_argument("--lock-wait-seconds", type=int, default=0)
    s.set_defaults(func=cmd_solve)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
