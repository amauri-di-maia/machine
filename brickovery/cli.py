from __future__ import annotations

import argparse
import json
import os
import re
import time
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
        # FIX: ingest_upstream_mapping expects explicit upstream args (checkout path, relpath, repo, ref, schema_hint)
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
            os.getenv("UPSTREAM_CHECKOUT_PATH")
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
    txt = _read_text(path).strip()
    if not txt:
        return []
    # Aceita ficheiro com <INVENTORY>...</INVENTORY> OU múltiplos <ITEM> soltos.
    try:
        root = ET.fromstring(txt)
    except ET.ParseError:
        root = ET.fromstring(f"<ROOT>\n{txt}\n</ROOT>")

    items = root.findall(".//ITEM") if root.tag != "ITEM" else [root]
    out: List[_RawItem] = []

    for it in items:
        part_id = (it.findtext("ITEMID") or "").strip()
        itemtype = (it.findtext("ITEMTYPE") or "P").strip() or "P"
        color_raw = (it.findtext("COLOR") or "").strip()
        qty_raw = (it.findtext("QTY") or "").strip()
        cond = (it.findtext("CONDITION") or "N").strip().upper() or "N"

        if not part_id:
            raise ValueError("Found ITEM missing ITEMID")

        try:
            qty = int(float(qty_raw)) if qty_raw else 0
        except Exception as e:
            raise ValueError(f"Invalid QTY='{qty_raw}' for ITEMID={part_id}") from e

        if qty <= 0:
            continue

        color_id: Optional[int] = None
        if color_raw:
            try:
                color_id = int(color_raw)
            except Exception as e:
                raise ValueError(f"Invalid COLOR='{color_raw}' for ITEMID={part_id}") from e

        if cond not in {"N", "U"}:
            raise ValueError(f"Invalid CONDITION='{cond}' for ITEMID={part_id} (expected N or U)")

        out.append(_RawItem(itemtype, part_id, color_id, qty, cond))

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
IDITEM_RE = re.compile(r"[?&]idItem=(\d+)", re.IGNORECASE)
IDITEM_JSON_RE = re.compile(r'"idItem"\s*:\s*(\d+)', re.IGNORECASE)


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
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except Exception as e:
        raise RuntimeError("beautifulsoup4 is required for scraping. Add 'beautifulsoup4' to requirements.txt") from e

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = [" ".join(l.split()) for l in text.splitlines() if l.strip()]

    in_sold = False
    out = {"new": {"qty": None, "avg": None}, "used": {"qty": None, "avg": None}}

    for line in lines:
        if "Past 6 Months Sales" in line:
            in_sold = True
            continue
        if in_sold and "Current Items for Sale" in line:
            break

        m = re.match(r"^(New|Used):\s*(.+)$", line, flags=re.IGNORECASE)
        if not (in_sold and m):
            continue
        kind = "new" if m.group(1).lower() == "new" else "used"
        payload = m.group(2)

        nums = re.findall(r"\d[\d,]*(?:\.\d+)?", payload)
        if len(nums) >= 4:
            qty = int(nums[1].replace(",", ""))
            avg = nums[3]
            if "," in avg and "." in avg:
                avg = avg.replace(",", "")
            elif "," in avg and "." not in avg:
                avg = avg.replace(",", ".")
            out[kind] = {"qty": qty, "avg": Decimal(avg)}
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
    raise RuntimeError(f"Failed to fetch {url} after {max_tries} tries") from last


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
                m = IDITEM_RE.search(html_cat) or IDITEM_JSON_RE.search(html_cat)
                if not m:
                    raise RuntimeError(f"Could not resolve idItem for {itype}:{pid}")
                id_item = int(m.group(1))

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

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
