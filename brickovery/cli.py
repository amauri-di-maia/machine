from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, fields, MISSING
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


# =========================
# Helpers (robustos)
# =========================

def _cfg_to_dict(cfg: Any) -> Dict[str, Any]:
    """Extrai um dict estável do cfg para hashing/auditoria, sem depender do formato interno."""
    if hasattr(cfg, "raw") and isinstance(getattr(cfg, "raw"), dict) and cfg.raw:
        return dict(cfg.raw)
    if hasattr(cfg, "model_dump"):
        return cfg.model_dump()
    # fallback extremo
    return json.loads(json.dumps(cfg, default=str))


def _build_dataclass_instance(cls: Any, data: Dict[str, Any]) -> Any:
    """
    Constrói um dataclass (RunManifest) de forma compatível mesmo se o schema do dataclass variar.
    Só passa campos existentes; falha com mensagem clara se faltar algum campo obrigatório.
    """
    kwargs: Dict[str, Any] = {}
    missing_required: List[str] = []

    for f in fields(cls):
        if f.name in data:
            kwargs[f.name] = data[f.name]
            continue

        has_default = f.default is not MISSING
        has_default_factory = getattr(f, "default_factory", MISSING) is not MISSING  # type: ignore[attr-defined]
        if has_default or has_default_factory:
            continue

        missing_required.append(f.name)

    if missing_required:
        raise RuntimeError(
            f"RunManifest schema mismatch: missing required fields {missing_required}. "
            f"Update cli.py manifest data to include these fields."
        )
    return cls(**kwargs)


def _ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


# =========================
# M1 parsing (dentro do CLI)
# =========================

@dataclass(frozen=True)
class _RawItem:
    bl_itemtype: str
    bl_part_id: str
    bl_color_id: Optional[int]
    qty: int
    condition: str


def _get_text(parent: ET.Element, tag: str) -> Optional[str]:
    el = parent.find(tag)
    if el is None or el.text is None:
        return None
    s = el.text.strip()
    return s if s else None


def _parse_search_xml(path: str) -> List[_RawItem]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"search.xml not found: {path}")

    txt = p.read_text(encoding="utf-8", errors="replace").strip()
    if not txt:
        raise ValueError(f"search.xml is empty: {path}")

    try:
        root = ET.fromstring(txt)
    except ET.ParseError:
        # muitos exportes vêm sem root — embrulhar
        root = ET.fromstring(f"<ROOT>\n{txt}\n</ROOT>")

    out: List[_RawItem] = []
    for item in root.findall(".//ITEM"):
        part_id = _get_text(item, "ITEMID")
        itemtype = (_get_text(item, "ITEMTYPE") or "P").strip().upper()
        color_raw = _get_text(item, "COLOR")
        qty_raw = _get_text(item, "QTY")
        cond = (_get_text(item, "CONDITION") or "N").strip().upper()

        if not part_id:
            raise ValueError("Found ITEM missing ITEMID")

        try:
            qty = int(float(qty_raw)) if qty_raw is not None else 0
        except Exception:
            raise ValueError(f"Invalid QTY='{qty_raw}' for ITEMID={part_id}")

        if qty <= 0:
            continue

        color_id: Optional[int] = None
        if color_raw is not None:
            try:
                color_id = int(color_raw)
            except Exception:
                raise ValueError(f"Invalid COLOR='{color_raw}' for ITEMID={part_id}")

        if cond not in {"N", "U"}:
            raise ValueError(f"Invalid CONDITION='{cond}' for ITEMID={part_id} (expected N or U)")

        out.append(_RawItem(itemtype, part_id, color_id, qty, cond))

    return out


def _aggregate_items(raw: List[_RawItem]) -> List[_RawItem]:
    agg: Dict[Tuple[str, str, Optional[int], str], int] = {}
    for r in raw:
        k = (r.bl_itemtype, r.bl_part_id, r.bl_color_id, r.condition)
        agg[k] = agg.get(k, 0) + r.qty

    out = [_RawItem(k[0], k[1], k[2], v, k[3]) for k, v in agg.items()]
    out.sort(key=lambda x: (x.bl_itemtype, x.bl_part_id, -1 if x.bl_color_id is None else x.bl_color_id, x.condition))
    return out


def _color_match_sql() -> str:
    return "((bl_color_id IS NULL AND ? IS NULL) OR (bl_color_id = ?))"


def _lookup_override(con, it: _RawItem) -> Optional[int]:
    q = f"""
      SELECT bo_boid
      FROM mapping_overrides
      WHERE bl_itemtype = ?
        AND bl_part_id = ?
        AND {_color_match_sql()}
      LIMIT 1;
    """
    row = con.execute(q, (it.bl_itemtype, it.bl_part_id, it.bl_color_id, it.bl_color_id)).fetchone()
    if row is None or row[0] is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def _lookup_upstream(con, it: _RawItem) -> Tuple[Optional[int], Optional[int]]:
    q = f"""
      SELECT bo_boid, weight_mg
      FROM up_mapping_mirror
      WHERE bl_itemtype = ?
        AND bl_part_id = ?
        AND {_color_match_sql()}
      LIMIT 1;
    """
    row = con.execute(q, (it.bl_itemtype, it.bl_part_id, it.bl_color_id, it.bl_color_id)).fetchone()
    if row is None:
        return None, None
    boid = row[0]
    wmg = row[1]
    try:
        boid_i = int(boid) if boid is not None else None
    except Exception:
        boid_i = None
    try:
        wmg_i = int(wmg) if wmg is not None else None
    except Exception:
        wmg_i = None
    return boid_i, wmg_i


def _lookup_weight_fallback(con, it: _RawItem) -> Optional[int]:
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
    if row is None or row[0] is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def _ensure_fill_later(con, it: _RawItem, reason: str) -> bool:
    now = utc_now_iso()
    q = """
      INSERT OR IGNORE INTO mapping_overrides
        (bl_itemtype, bl_part_id, bl_color_id, bo_boid, status, reason, created_ts, updated_ts)
      VALUES
        (?, ?, ?, NULL, 'fill_later', ?, ?, ?);
    """
    cur = con.execute(q, (it.bl_itemtype, it.bl_part_id, it.bl_color_id, reason, now, now))
    return cur.rowcount == 1


# =========================
# Commands
# =========================

def cmd_bootstrap(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)

    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    logger = JsonlLogger(cfg.paths.logs_dir, run_id=run_id, slice_id=slice_id)

    lock.acquire(owner_run_id=run_id, owner_info={"action": "bootstrap"}, wait_seconds=args.lock_wait_seconds)
    try:
        logger.log("bootstrap.start", config=args.config)

        # Optional: pull (apenas se não desativado)
        if getattr(cfg.github_sync, "enabled", False) and getattr(cfg.github_sync, "pull", False) and not args.no_pull:
            logger.log("git.pull.start")
            # sync_repo exige commit_message; passamos dummy e do_commit/do_push False
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

        schema_hint = {}
        if hasattr(cfg.upstream, "schema_hint") and cfg.upstream.schema_hint:
            schema_hint = cfg.upstream.schema_hint

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

        # Shipping
        sn_hash, sn_n = ingest_shipping_bands(con, cfg.tables.shipping_normal_txt, service_level="normal", dest_country="PT")
        logger.log("shipping.ingested", service_level="normal", bands=sn_n, source_hash=sn_hash)

        sr_hash, sr_n = ingest_shipping_bands(con, cfg.tables.shipping_registered_txt, service_level="registered", dest_country="PT")
        logger.log("shipping.ingested", service_level="registered", bands=sr_n, source_hash=sr_hash)

        # Rarity rules
        rr_hash, rr_n = ingest_rarity_rules(con, cfg.tables.rarity_rules_txt)
        logger.log("rarity.ingested", rules=rr_n, source_hash=rr_hash)

        # Manifest (compatível com diferentes versões do dataclass)
        cfg_hash = sha256_json(_cfg_to_dict(cfg))
        manifest_data = {
            "run_id": run_id,
            "slice_id": slice_id,
            "created_ts": utc_now_iso(),
            "config_hash": cfg_hash,
            "upstream_repo": cfg.upstream.repo,
            "upstream_ref": cfg.upstream.ref,
            "upstream_commit_sha": up_res.get("upstream_commit_sha", ""),
            "upstream_db_relpath": cfg.upstream.db_relpath,
            "upstream_db_sha256": up_res.get("upstream_db_sha256", ""),
            "notes": "M0 bootstrap",
        }
        manifest_obj = _build_dataclass_instance(RunManifest, manifest_data)
        manifest_path = write_manifest(manifest_obj, cfg.paths.manifests_dir)

        con.execute(
            "INSERT OR REPLACE INTO run_manifest(run_id, slice_id, manifest_json, created_ts) VALUES(?,?,?,?);",
            (run_id, slice_id, Path(manifest_path).read_text(encoding="utf-8"), utc_now_iso()),
        )
        con.commit()
        con.close()

        logger.log("bootstrap.done", run_id=run_id, slice_id=slice_id, manifest_path=str(manifest_path))

        # Optional: commit/push (apenas se não desativado)
        if getattr(cfg.github_sync, "enabled", False) and not args.no_commit:
            msg_tmpl = getattr(cfg.github_sync, "commit_message_template", "brickovery: {action} (run_id={run_id}, slice_id={slice_id})")
            msg = msg_tmpl.format(
                action="bootstrap",
                run_id=run_id,
                slice_id=slice_id,
                upstream_commit_sha=up_res.get("upstream_commit_sha", ""),
            )
            do_push = bool(getattr(cfg.github_sync, "push", False)) and not args.no_push
            sync_res = sync_repo(
                repo_root=cfg.paths.repo_root,
                do_pull=False,
                do_commit=bool(getattr(cfg.github_sync, "commit", True)),
                do_push=do_push,
                commit_message=msg,
                user_name=getattr(cfg.github_sync, "git_user_name", "github-actions[bot]"),
                user_email=getattr(cfg.github_sync, "git_user_email", "41898282+github-actions[bot]@users.noreply.github.com"),
                fail_safe_on_push_conflict=getattr(cfg.github_sync, "fail_safe_on_push_conflict", True),
            )
            logger.log("git.sync.done", status=sync_res.status, did_commit=sync_res.did_commit, did_push=sync_res.did_push)

        return 0

    finally:
        lock.release()


def cmd_normalize_input(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)

    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    logger = JsonlLogger(cfg.paths.logs_dir, run_id=run_id, slice_id=slice_id)

    lock.acquire(owner_run_id=run_id, owner_info={"action": "normalize-input"}, wait_seconds=args.lock_wait_seconds)
    try:
        logger.log("normalize.start", input_path=args.input)

        con = connect(cfg.paths.sqlite_db)
        migrate(con)

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
            boid = _lookup_override(con, it)
            mapping_source = "override" if boid is not None else "none"

            up_boid, up_wmg = _lookup_upstream(con, it)
            if boid is None and up_boid is not None:
                boid = up_boid
                mapping_source = "upstream"

            weight_mg = up_wmg if up_wmg is not None else _lookup_weight_fallback(con, it)

            mapping_status = "OK" if boid is not None else "MISSING_BOID"
            weight_status = "OK" if (weight_mg is not None and weight_mg > 0) else "MISSING_WEIGHT"

            if mapping_status == "OK":
                stats["mapped_ok"] += 1
            else:
                stats["missing_boid"] += 1
                if _ensure_fill_later(con, it, reason="missing boid in upstream/override"):
                    stats["overrides_created"] += 1

            if weight_status == "OK":
                stats["weight_ok"] += 1
                total_weight_mg += int(weight_mg) * int(it.qty)
            else:
                stats["missing_weight"] += 1

            normalized.append(
                {
                    "bl_itemtype": it.bl_itemtype,
                    "bl_part_id": it.bl_part_id,
                    "bl_color_id": it.bl_color_id,
                    "condition": it.condition,
                    "qty": it.qty,
                    "bo_boid": boid,
                    "weight_mg": weight_mg,
                    "mapping_source": mapping_source,
                    "mapping_status": mapping_status,
                    "weight_status": weight_status,
                }
            )

        con.commit()

        # outputs
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        normalized_path = out_dir / f"normalized_items.{run_id}.json"
        normalized_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        input_sha = sha256_file(args.input)
        normalized_sha = sha256_file(str(normalized_path))
        normalized_hash = sha256_json({"items": normalized})

        input_manifest = {
            "run_id": run_id,
            "slice_id": slice_id,
            "created_ts": utc_now_iso(),
            "input_path": args.input,
            "input_sha256": input_sha,
            "normalized_items_path": str(normalized_path),
            "normalized_items_sha256": normalized_sha,
            "normalized_items_hash": normalized_hash,
            "stats": stats,
            "total_weight_mg": total_weight_mg,
        }
        input_manifest["manifest_hash"] = sha256_json(input_manifest)

        input_manifest_path = out_dir / f"input_manifest.{run_id}.json"
        input_manifest_path.write_text(json.dumps(input_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        # Run manifest (compatível)
        cfg_hash = sha256_json(_cfg_to_dict(cfg))
        manifest_data = {
            "run_id": run_id,
            "slice_id": slice_id,
            "created_ts": utc_now_iso(),
            "config_hash": cfg_hash,
            "notes": "M1 normalize-input",
            "input_path": args.input,
            "input_sha256": input_sha,
            "input_items_hash": normalized_hash,
            "normalized_items_path": str(normalized_path),
            "normalized_items_sha256": normalized_sha,
            "missing_mapping_count": stats["missing_boid"],
            "total_weight_mg": total_weight_mg,
            # upstream fields se o dataclass exigir
            "upstream_repo": getattr(cfg.upstream, "repo", ""),
            "upstream_ref": getattr(cfg.upstream, "ref", ""),
            "upstream_commit_sha": getattr(cfg.upstream, "ref", ""),
            "upstream_db_relpath": getattr(cfg.upstream, "db_relpath", ""),
            "upstream_db_sha256": "",
        }
        manifest_obj = _build_dataclass_instance(RunManifest, manifest_data)
        manifest_path = write_manifest(manifest_obj, cfg.paths.manifests_dir)

        con.execute(
            "INSERT OR REPLACE INTO run_manifest(run_id, slice_id, manifest_json, created_ts) VALUES(?,?,?,?);",
            (run_id, slice_id, Path(manifest_path).read_text(encoding="utf-8"), utc_now_iso()),
        )
        con.commit()
        con.close()

        logger.log("normalize.done", **stats, total_weight_mg=total_weight_mg, normalized_path=str(normalized_path), input_manifest_path=str(input_manifest_path))
        return 0

    finally:
        lock.release()


# =========================
# Parser
# =========================

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
    n = sub.add_parser("normalize-input", help="M1: parse + normalize search.xml (creates overrides if missing BOID)")
    n.add_argument("--config", required=True)
    n.add_argument("--input", default="inputs/search.xml")
    n.add_argument("--output-dir", default="outputs/inputs")
    n.add_argument("--run-id", default=None)
    n.add_argument("--slice-id", default="0")
    n.add_argument("--lock-ttl-seconds", type=int, default=3600)
    n.add_argument("--lock-wait-seconds", type=int, default=0)
    n.set_defaults(func=cmd_normalize_input)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
