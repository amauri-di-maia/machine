from __future__ import annotations

import argparse
import json
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass, fields, MISSING
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config.loader import load_config
from .repositories.db import connect, migrate
from .github_sync.lock import RepoLock
from .github_sync.sync import sync_repo
from .logging.logger import JsonlLogger
from .io.hashing import sha256_json, sha256_file
from .io.timeutil import make_run_id, utc_now_iso
from .io.manifest import RunManifest, write_manifest

from .ingest.upstream import ingest_upstream_mapping
from .ingest.shipping import ingest_shipping_bands
from .ingest.rarity_rules import ingest_rarity_rules


# ============================================================================
# Helpers
# ============================================================================

def _cfg_to_dict(cfg: Any) -> Dict[str, Any]:
    """Extrai um dict estável do cfg para hashing/auditoria, sem depender do formato interno."""
    if hasattr(cfg, "raw") and isinstance(getattr(cfg, "raw"), dict) and cfg.raw:
        return dict(cfg.raw)
    if hasattr(cfg, "model_dump"):
        return cfg.model_dump()
    if hasattr(cfg, "dict"):
        return cfg.dict()
    return json.loads(json.dumps(cfg, default=str))


def _build_dataclass_instance(cls: Any, data: Dict[str, Any]) -> Any:
    """Constrói um dataclass (RunManifest) de forma compatível mesmo se o schema variar.
    Falha de forma explícita se faltar um campo obrigatório sem default.
    """
    allowed = {f.name for f in fields(cls)}
    out: Dict[str, Any] = {}
    for k, v in data.items():
        if k in allowed:
            out[k] = v

    missing_required: List[str] = []
    for f in fields(cls):
        if f.name in out:
            continue
        if f.default is not MISSING:
            out[f.name] = f.default
            continue
        if getattr(f, "default_factory", MISSING) is not MISSING:  # type: ignore[attr-defined]
            out[f.name] = f.default_factory()  # type: ignore[misc]
            continue
        missing_required.append(f.name)

    if missing_required:
        raise RuntimeError(
            f"RunManifest missing required fields: {missing_required}. " 
            f"Provide them in manifest_data or add defaults to RunManifest."
        )
    return cls(**out)


def _color_match_sql() -> str:
    return "((bl_color_id IS NULL AND ? IS NULL) OR (bl_color_id = ?))"


def _normalize_boid(v: Any) -> Optional[str]:
    """BOID pode ser numérico ou texto (ex.: '656416-20'). Preserva como string."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


# ============================================================================
# Input model
# ============================================================================

@dataclass(frozen=True)
class _RawItem:
    bl_itemtype: str
    bl_part_id: str
    bl_color_id: Optional[int]
    qty: int
    condition: str


def _parse_search_xml(path: str) -> List[_RawItem]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"search.xml not found: {path}")

    txt = p.read_text(encoding="utf-8", errors="replace").strip()
    if not txt:
        raise ValueError(f"search.xml is empty: {path}")

    # muitos exportes vêm sem root — embrulhar
    try:
        root = ET.fromstring(txt)
    except ET.ParseError:
        root = ET.fromstring(f"<ROOT>\n{txt}\n</ROOT>")

    out: List[_RawItem] = []
    for item in root.findall(".//ITEM"):
        part_id = (item.findtext("ITEMID") or "").strip()
        itemtype = (item.findtext("ITEMTYPE") or "").strip() or "P"
        color_raw = item.findtext("COLOR")
        qty_raw = item.findtext("QTY")
        cond = (item.findtext("CONDITION") or "N").strip().upper()

        if not part_id:
            raise ValueError("Missing ITEMID in one ITEM")

        if not qty_raw or not qty_raw.strip().isdigit():
            raise ValueError(f"Invalid QTY='{qty_raw}' for ITEMID={part_id}")
        qty = int(qty_raw.strip())
        if qty <= 0:
            raise ValueError(f"Invalid QTY='{qty_raw}' for ITEMID={part_id} (must be >0)")

        color_id: Optional[int] = None
        if color_raw is not None and str(color_raw).strip() != "":
            try:
                color_id = int(str(color_raw).strip())
            except Exception:
                raise ValueError(f"Invalid COLOR='{color_raw}' for ITEMID={part_id}")

        if cond not in {"N", "U"}:
            raise ValueError(f"Invalid CONDITION='{cond}' for ITEMID={part_id} (expected N or U)")

        out.append(_RawItem(itemtype, part_id, color_id, qty, cond))

    return out


def _aggregate_items(items: List[_RawItem]) -> List[_RawItem]:
    """Agrupa linhas idênticas somando qty, com ordenação determinística."""
    acc: Dict[Tuple[str, str, int, str], int] = {}
    for it in items:
        key = (it.bl_itemtype, it.bl_part_id, -1 if it.bl_color_id is None else int(it.bl_color_id), it.condition)
        acc[key] = acc.get(key, 0) + int(it.qty)

    out: List[_RawItem] = []
    for (t, pid, cid, cond), qty in sorted(acc.items(), key=lambda x: (x[0][0], x[0][1], x[0][2], x[0][3])):
        out.append(_RawItem(t, pid, None if cid == -1 else cid, qty, cond))
    return out


# ============================================================================
# DB lookups for mapping + weights
# ============================================================================

def _lookup_override(con, it: _RawItem) -> Optional[str]:
    row = con.execute(
        f"""
        SELECT bo_boid
        FROM mapping_overrides
        WHERE bl_itemtype = ?
          AND bl_part_id = ?
          AND {_color_match_sql()}
        LIMIT 1;
        """,
        (it.bl_itemtype, it.bl_part_id, it.bl_color_id, it.bl_color_id),
    ).fetchone()
    if not row:
        return None
    return _normalize_boid(row[0])


def _lookup_upstream(con, it: _RawItem) -> Tuple[Optional[str], Optional[int]]:
    row = con.execute(
        f"""
        SELECT bo_boid, weight_mg
        FROM up_mapping_mirror
        WHERE bl_itemtype = ?
          AND bl_part_id = ?
          AND {_color_match_sql()}
        LIMIT 1;
        """,
        (it.bl_itemtype, it.bl_part_id, it.bl_color_id, it.bl_color_id),
    ).fetchone()
    if not row:
        return None, None
    boid = _normalize_boid(row[0])
    wmg = row[1]
    try:
        wmg_i = int(wmg) if wmg is not None else None
    except Exception:
        wmg_i = None
    return boid, wmg_i


def _lookup_weight_fallback(con, it: _RawItem) -> Optional[int]:
    row = con.execute(
        """
        SELECT weight_mg
        FROM up_mapping_mirror
        WHERE bl_itemtype = ? AND bl_part_id = ?
        ORDER BY CASE WHEN bl_color_id IS NULL THEN 1 ELSE 0 END, bl_color_id
        LIMIT 1;
        """,
        (it.bl_itemtype, it.bl_part_id),
    ).fetchone()
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def _ensure_fill_later(con, it: _RawItem, reason: str) -> bool:
    now = utc_now_iso()
    cur = con.execute(
        """
        INSERT OR IGNORE INTO mapping_overrides
          (bl_itemtype, bl_part_id, bl_color_id, bo_boid, status, reason, created_ts, updated_ts)
        VALUES (?, ?, ?, NULL, 'fill_later', ?, ?, ?);
        """,
        (it.bl_itemtype, it.bl_part_id, it.bl_color_id, reason, now, now),
    )
    return cur.rowcount == 1


# ============================================================================
# M0: bootstrap
# ============================================================================

def cmd_bootstrap(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)

    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    logger = JsonlLogger(cfg.paths.logs_dir, run_id=run_id, slice_id=slice_id)

    lock.acquire(owner_run_id=run_id, owner_info={"action": "bootstrap"}, wait_seconds=args.lock_wait_seconds)
    try:
        logger.log("bootstrap.start", config=args.config)

        # Pull opcional (quando github_sync.enabled)
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

        # schema_hint (compatível com pydantic/dataclass)
        schema_hint: Dict[str, Any] = {}
        hint_obj = getattr(cfg.upstream, "schema_hint", None)
        if hint_obj:
            if isinstance(hint_obj, dict):
                schema_hint = hint_obj
            elif hasattr(hint_obj, "model_dump"):
                schema_hint = hint_obj.model_dump()
            elif hasattr(hint_obj, "dict"):
                schema_hint = hint_obj.dict()
            else:
                schema_hint = dict(hint_obj)

        upstream_checkout_path = getattr(cfg.upstream, "checkout_path", None) or args.upstream_checkout_path

        up_res = ingest_upstream_mapping(
            con_work=con,
            upstream_checkout_path=upstream_checkout_path,
            upstream_db_relpath=cfg.upstream.db_relpath,
            upstream_repo=cfg.upstream.repo,
            upstream_ref=cfg.upstream.ref,
            schema_hint=schema_hint,
        )
        logger.log("upstream.ingested", **up_res)

        # Shipping bands
        sn_hash, sn_n = ingest_shipping_bands(con, cfg.tables.shipping_normal_txt, service_level="normal", dest_country="PT")
        logger.log("shipping.ingested", service_level="normal", bands=sn_n, source_hash=sn_hash)

        sr_hash, sr_n = ingest_shipping_bands(con, cfg.tables.shipping_registered_txt, service_level="registered", dest_country="PT")
        logger.log("shipping.ingested", service_level="registered", bands=sr_n, source_hash=sr_hash)

        # Rarity rules
        rr_hash, rr_n = ingest_rarity_rules(con, cfg.tables.rarity_rules_txt)
        logger.log("rarity.ingested", rules=rr_n, source_hash=rr_hash)

        con.commit()
        con.close()

        # Run manifest
        cfg_hash = sha256_json(_cfg_to_dict(cfg))
        up_row = con.execute("SELECT upstream_commit_sha, upstream_db_sha256 FROM upstream_state WHERE id=1").fetchone()
        up_commit = up_row[0] if up_row else ""
        up_dbsha = up_row[1] if up_row else ""
        manifest_data = {
            "run_id": run_id,
            "slice_id": slice_id,
            "created_ts": utc_now_iso(),
            "config_hash": cfg_hash,
            "notes": "M1 normalize-input",
            "input_path": args.input,
            "input_hash": input_manifest["input_sha256"],
            "upstream_repo": getattr(cfg.upstream, "repo", ""),
            "upstream_ref": getattr(cfg.upstream, "ref", ""),
            "upstream_commit_sha": up_commit,
            "upstream_db_relpath": getattr(cfg.upstream, "db_relpath", ""),
            "upstream_db_sha256": up_dbsha,
        }
        manifest = _build_dataclass_instance(RunManifest, manifest_data)
        write_manifest(cfg.paths.manifests_dir, manifest)

        logger.log("normalize.done", **input_manifest["stats"], total_weight_mg=total_weight_mg)
        return 0

    finally:
        lock.release()


# ============================================================================
# M2: refresh-cache (BrickLink HTML)
# ============================================================================

def _ensure_market_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS market_metrics (
          platform TEXT NOT NULL,
          bl_itemtype TEXT NOT NULL,
          bl_part_id TEXT NOT NULL,
          bl_color_id INTEGER,
          condition TEXT NOT NULL,                 -- 'N' or 'U'
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
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS market_offers (
          platform TEXT NOT NULL,
          snapshot_id TEXT NOT NULL,
          bl_itemtype TEXT NOT NULL,
          bl_part_id TEXT NOT NULL,
          bl_color_id INTEGER,
          condition TEXT NOT NULL,                 -- 'N' or 'U'
          store_key TEXT,
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
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_market_offers_key ON market_offers(platform, bl_itemtype, bl_part_id, bl_color_id, condition);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_market_offers_snap ON market_offers(snapshot_id);")


def _market_last_fetched_ts(con, *, platform: str, bl_itemtype: str, bl_part_id: str, bl_color_id: Optional[int], condition: str) -> Optional[str]:
    row = con.execute(
        """
        SELECT fetched_ts
        FROM market_metrics
        WHERE platform=? AND bl_itemtype=? AND bl_part_id=? AND
              ((bl_color_id IS NULL AND ? IS NULL) OR (bl_color_id=?)) AND condition=?
        """,
        (platform, bl_itemtype, bl_part_id, bl_color_id, bl_color_id, condition),
    ).fetchone()
    return row[0] if row else None


def _iso_parse(ts: str):
    from datetime import datetime, timezone
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _is_fresh(ts: Optional[str], ttl_hours: int) -> bool:
    if not ts:
        return False
    from datetime import datetime, timezone, timedelta
    dt = _iso_parse(ts)
    return (datetime.now(timezone.utc) - dt) <= timedelta(hours=ttl_hours)


def cmd_refresh_cache(args: argparse.Namespace) -> int:
    """Atualiza métricas e snapshot de ofertas (BrickLink HTML) com TTL em horas."""
    try:
        import re
        import time
        from dataclasses import dataclass
        from decimal import Decimal
        from typing import Optional as _Optional, Dict as _Dict, Tuple as _Tuple, List as _List
        from urllib.parse import urljoin
        import requests
        from bs4 import BeautifulSoup
    except Exception as e:
        raise RuntimeError("Missing dependencies for refresh-cache (requests, beautifulsoup4). Install requirements.") from e

    cfg = load_config(args.config)
    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id
    snapshot_id = args.snapshot_id or run_id

    ttl_hours = int(args.ttl_hours)
    cookie_env = args.cookie_env_var
    cookie = os.getenv(cookie_env, None)

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    logger = JsonlLogger(cfg.paths.logs_dir, run_id=run_id, slice_id=slice_id)

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

    @dataclass(frozen=True)
    class Offer:
        country_iso2: _Optional[str]
        ships_to_me: _Optional[bool]
        qty: int
        price: _Optional[Decimal]
        is_estimate: bool
        store_url: _Optional[str]

    @dataclass(frozen=True)
    class Sold6M:
        total_qty: _Optional[int]
        avg_price: _Optional[Decimal]

    @dataclass(frozen=True)
    class CurrentStats:
        total_lots: int
        total_qty: int
        min_price: _Optional[Decimal]
        min_price_is_estimate: _Optional[bool]

    def parse_price_cell(text: str) -> _Tuple[_Optional[Decimal], bool]:
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

    def parse_offers_from_html(html: str) -> _List[Offer]:
        soup = BeautifulSoup(html, "html.parser")
        offers: _List[Offer] = []
        for tr in soup.find_all("tr"):
            a = tr.find("a", href=STORE_RE)
            if not a:
                continue
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            country_iso2 = None
            flag_img = tr.find("img", src=FLAG_RE)
            if flag_img and flag_img.get("src"):
                mm = FLAG_RE.search(flag_img["src"])
                if mm:
                    country_iso2 = mm.group(1).upper()

            ships_to_me = None
            box_img = tr.find("img", src=BOX_RE)
            if box_img and box_img.get("src"):
                mm = BOX_RE.search(box_img["src"])
                if mm:
                    ships_to_me = (mm.group(1).upper() == "Y")

            qty_txt = tds[1].get_text(strip=True)
            qty = int(qty_txt) if qty_txt.isdigit() else 0

            price = None
            is_est = False
            for td in (tds[-1], *tds[:-1]):
                p, est = parse_price_cell(td.get_text(" ", strip=True))
                if p is not None:
                    price, is_est = p, est
                    break

            store_url = None
            href = a.get("href")
            if href:
                store_url = href.replace("http://", "https://")
                store_url = urljoin("https://www.bricklink.com/", store_url)

            offers.append(Offer(country_iso2, ships_to_me, qty, price, is_est, store_url))
        return offers

    def filter_offers_eu_ships_strict(offers: _List[Offer]) -> _List[Offer]:
        out: _List[Offer] = []
        for o in offers:
            if o.ships_to_me is not True:
                continue
            if not o.country_iso2:
                continue
            c = o.country_iso2.upper()
            if c in EXCLUDE_ISO2:
                continue
            if c not in EU_ISO2:
                continue
            out.append(o)
        return out

    def compute_current_stats(offers: _List[Offer]) -> CurrentStats:
        total_lots = len(offers)
        total_qty = sum(o.qty for o in offers)
        min_price = None
        min_is_est = None
        for o in offers:
            if o.price is None:
                continue
            if (min_price is None) or (o.price < min_price):
                min_price = o.price
                min_is_est = o.is_estimate
        return CurrentStats(total_lots, total_qty, min_price, min_is_est)

    def parse_price_guide_summary_sold6m(html: str) -> _Dict[str, Sold6M]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n")
        lines = [" ".join(l.split()) for l in text.splitlines() if l.strip()]
        in_sold = False
        out: _Dict[str, Sold6M] = {"new": Sold6M(None, None), "used": Sold6M(None, None)}
        for line in lines:
            if "Past 6 Months Sales" in line:
                in_sold = True
                continue
            if in_sold and "Current Items for Sale" in line:
                break
            mm = re.match(r"^(New|Used):\s*(.+)$", line, flags=re.IGNORECASE)
            if not (in_sold and mm):
                continue
            kind = "new" if mm.group(1).lower() == "new" else "used"
            payload = mm.group(2)
            nums = re.findall(r"\d[\d,]*(?:\.\d+)?", payload)
            if len(nums) >= 4:
                total_qty = int(nums[1].replace(",", ""))
                avg = nums[3]
                if "," in avg and "." in avg:
                    avg = avg.replace(",", "")
                elif "," in avg and "." not in avg:
                    avg = avg.replace(",", ".")
                out[kind] = Sold6M(total_qty, Decimal(avg))
        return out

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.bricklink.com/",
    }
    if cookie:
        headers["Cookie"] = cookie

    def fetch_html(url: str, params: dict, delay_s: float, max_tries: int = 5, timeout_s: int = 25) -> str:
        if delay_s > 0:
            time.sleep(delay_s)
        last_err = None
        for attempt in range(1, max_tries + 1):
            try:
                r = session.get(url, params=params, headers=headers, timeout=timeout_s)
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(12.0, (2.0 ** attempt)))
                    continue
                r.raise_for_status()
                return r.text
            except Exception as e:
                last_err = e
                time.sleep(min(12.0, (2.0 ** attempt)))
        raise RuntimeError(f"Failed to fetch {url} after {max_tries} tries") from last_err

    CATALOG_ITEM_URL = "https://www.bricklink.com/v2/catalog/catalogitem.page"
    OFFERS_URL = "https://www.bricklink.com/v2/catalog/catalogitem_pgtab.page"
    SOLD_URL = "https://www.bricklink.com/priceGuideSummary.asp"

    def resolve_id_item(itemtype: str, part_id: str) -> int:
        html = fetch_html(CATALOG_ITEM_URL, params={itemtype.upper(): part_id}, delay_s=0.0)
        mm = IDITEM_RE.search(html)
        if mm:
            return int(mm.group(1))
        mm = IDITEM_JSON_RE.search(html)
        if mm:
            return int(mm.group(1))
        allm = IDITEM_RE.findall(html)
        if allm:
            return int(allm[0])
        raise RuntimeError(f"Could not resolve BrickLink idItem for {itemtype}:{part_id}")

    def fetch_sold_6m(itemtype: str, id_item: int, color_id: int) -> _Dict[str, Sold6M]:
        html = fetch_html(
            SOLD_URL,
            params={"a": itemtype.upper(), "itemID": str(id_item), "colorID": str(color_id), "vatInc": "Y", "vcID": "2"},
            delay_s=0.0,
        )
        return parse_price_guide_summary_sold6m(html)

    def fetch_current_offers(id_item: int, color_id: int, st: int, delay_s: float) -> _Tuple[_List[Offer], CurrentStats]:
        html = fetch_html(
            OFFERS_URL,
            params={
                "idItem": str(id_item),
                "idColor": str(color_id),
                "st": str(st),   # 1 new, 2 used
                "gm": "1", "gc": "0", "ei": "0",
                "prec": "3",
                "showflag": "1",
                "showbulk": "1",
                "currency": "2",
            },
            delay_s=delay_s,
        )
        offers = filter_offers_eu_ships_strict(parse_offers_from_html(html))
        return offers, compute_current_stats(offers)

    def dec_to_txt(v: _Optional[Decimal]) -> _Optional[str]:
        if v is None:
            return None
        return format(v, "f")

    if not cookie:
        raise RuntimeError(f"{cookie_env} is required for strict ships-to-PT filtering (box16Y).")

    lock.acquire(owner_run_id=run_id, owner_info={"action": "refresh-cache"}, wait_seconds=args.lock_wait_seconds)
    try:
        logger.log("m2.start", ttl_hours=ttl_hours, snapshot_id=snapshot_id)

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

        items = json.loads(Path(norm_path).read_text(encoding="utf-8"))
        fetched_ts = utc_now_iso()
        parser_version = "bl_html_v1"
        data_source = "scrape"

        refreshed = 0
        skipped = 0
        errors = 0

        for it in items:
            bl_itemtype = it["bl_itemtype"]
            bl_part_id = it["bl_part_id"]
            bl_color_id = it.get("bl_color_id")
            if bl_color_id is None:
                skipped += 1
                logger.log("m2.skip", reason="missing_color", bl_itemtype=bl_itemtype, bl_part_id=bl_part_id)
                continue

            last_n = _market_last_fetched_ts(con, platform="BL", bl_itemtype=bl_itemtype, bl_part_id=bl_part_id, bl_color_id=int(bl_color_id), condition="N")
            last_u = _market_last_fetched_ts(con, platform="BL", bl_itemtype=bl_itemtype, bl_part_id=bl_part_id, bl_color_id=int(bl_color_id), condition="U")

            if (not args.force) and _is_fresh(last_n, ttl_hours) and _is_fresh(last_u, ttl_hours):
                skipped += 1
                continue

            try:
                id_item = resolve_id_item(bl_itemtype, bl_part_id)
                sold = fetch_sold_6m(bl_itemtype, id_item, int(bl_color_id))
                offers_n, stats_n = fetch_current_offers(id_item, int(bl_color_id), st=1, delay_s=float(args.delay_s))
                offers_u, stats_u = fetch_current_offers(id_item, int(bl_color_id), st=2, delay_s=float(args.delay_s))

                # metrics N/U
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
                        "BL", bl_itemtype, bl_part_id, int(bl_color_id), "N",
                        sold["new"].total_qty, dec_to_txt(sold["new"].avg_price),
                        dec_to_txt(stats_n.min_price), 1 if stats_n.min_price_is_estimate else 0 if stats_n.min_price_is_estimate is not None else None,
                        int(stats_n.total_lots), int(stats_n.total_qty),
                        fetched_ts, snapshot_id, data_source, parser_version,
                    ),
                )
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
                        "BL", bl_itemtype, bl_part_id, int(bl_color_id), "U",
                        sold["used"].total_qty, dec_to_txt(sold["used"].avg_price),
                        dec_to_txt(stats_u.min_price), 1 if stats_u.min_price_is_estimate else 0 if stats_u.min_price_is_estimate is not None else None,
                        int(stats_u.total_lots), int(stats_u.total_qty),
                        fetched_ts, snapshot_id, data_source, parser_version,
                    ),
                )

                # replace offers for this key (platform+item+color+cond)
                for cond in ("N", "U"):
                    con.execute(
                        "DELETE FROM market_offers WHERE platform=? AND bl_itemtype=? AND bl_part_id=? AND bl_color_id=? AND condition=?",
                        ("BL", bl_itemtype, bl_part_id, int(bl_color_id), cond),
                    )

                for o in offers_n:
                    con.execute(
                        """
                        INSERT INTO market_offers(
                          platform, snapshot_id, bl_itemtype, bl_part_id, bl_color_id, condition,
                          store_key, store_url, country_iso2, ships_to_me,
                          qty, price_eur, is_estimate,
                          fetched_ts, data_source, parser_version
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            "BL", snapshot_id, bl_itemtype, bl_part_id, int(bl_color_id), "N",
                            o.store_url, o.store_url, o.country_iso2,
                            1 if o.ships_to_me else 0 if o.ships_to_me is not None else None,
                            int(o.qty), dec_to_txt(o.price), 1 if o.is_estimate else 0,
                            fetched_ts, data_source, parser_version,
                        ),
                    )
                for o in offers_u:
                    con.execute(
                        """
                        INSERT INTO market_offers(
                          platform, snapshot_id, bl_itemtype, bl_part_id, bl_color_id, condition,
                          store_key, store_url, country_iso2, ships_to_me,
                          qty, price_eur, is_estimate,
                          fetched_ts, data_source, parser_version
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            "BL", snapshot_id, bl_itemtype, bl_part_id, int(bl_color_id), "U",
                            o.store_url, o.store_url, o.country_iso2,
                            1 if o.ships_to_me else 0 if o.ships_to_me is not None else None,
                            int(o.qty), dec_to_txt(o.price), 1 if o.is_estimate else 0,
                            fetched_ts, data_source, parser_version,
                        ),
                    )

                refreshed += 1

            except Exception as e:
                errors += 1
                logger.log("m2.error", bl_itemtype=bl_itemtype, bl_part_id=bl_part_id, bl_color_id=bl_color_id, error=str(e))

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
            "cookie_env_var": cookie_env,
            "parser_version": parser_version,
            "data_source": data_source,
        }
        (out_dir / f"refresh_report.{run_id}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        logger.log("m2.done", **report)
        return 0

    finally:
        lock.release()


# ============================================================================
# CLI entrypoint
# ============================================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="brickovery")
    sub = p.add_subparsers(dest="cmd", required=True)

    # M0
    b = sub.add_parser("bootstrap", help="M0: ingest upstream mapping + shipping bands + rarity rules")
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

    # M1
    n = sub.add_parser("normalize-input", help="M1: parse search.xml + normalize + input_manifest")
    n.add_argument("--config", required=True)
    n.add_argument("--input", required=True)
    n.add_argument("--output-dir", required=True)
    n.add_argument("--run-id", default=None)
    n.add_argument("--slice-id", default="0")
    n.add_argument("--lock-ttl-seconds", type=int, default=3600)
    n.add_argument("--lock-wait-seconds", type=int, default=0)
    n.set_defaults(func=cmd_normalize_input)

    # M2
    m2 = sub.add_parser("refresh-cache", help="M2: refresh market cache + metrics (TTL enforced)")
    m2.add_argument("--config", required=True)
    m2.add_argument("--normalized-items", default=None)
    m2.add_argument("--run-id", default=None)
    m2.add_argument("--slice-id", default="0")
    m2.add_argument("--snapshot-id", default=None)
    m2.add_argument("--ttl-hours", default="6")
    m2.add_argument("--force", action="store_true")
    m2.add_argument("--cookie-env-var", default="BRICKLINK_COOKIE")
    m2.add_argument("--delay-s", default="0.9")
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
