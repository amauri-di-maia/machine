from __future__ import annotations

import argparse
from pathlib import Path

from .config.loader import load_config
from .repositories.db import connect, migrate
from .github_sync.lock import RepoLock, RepoLockError
from .github_sync.sync import sync_repo
from .logging.logger import JsonlLogger
from .io.hashing import sha256_json
from .io.timeutil import make_run_id, utc_now_iso
from .io.manifest import RunManifest, write_manifest

from .ingest.upstream import ingest_upstream_mapping
from .ingest.shipping import ingest_shipping_bands
from .ingest.rarity_rules import ingest_rarity_rules

from pathlib import Path
import json

from .io.hashing import sha256_file
from .io.search_xml import parse_search_xml
from .normalize.normalize_input import aggregate_requested, normalize_items, normalized_items_hash


def cmd_bootstrap(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    repo_root = cfg.paths.repo_root
    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id or "0"
    logger = JsonlLogger(cfg.paths.logs_dir, run_id, slice_id)

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds)
    try:
        lock.acquire(owner_run_id=run_id, owner_info={"slice_id": slice_id}, wait_seconds=args.lock_wait_seconds)
    except RepoLockError as e:
        raise SystemExit(str(e))

    try:
        logger.log("bootstrap.start", config_path=args.config)

        Path(cfg.paths.manifests_dir).mkdir(parents=True, exist_ok=True)
        Path(cfg.paths.logs_dir).mkdir(parents=True, exist_ok=True)
        Path(cfg.paths.sqlite_db).parent.mkdir(parents=True, exist_ok=True)

        con = connect(cfg.paths.sqlite_db)
        migrate(con)
        logger.log("db.migrated", db_path=cfg.paths.sqlite_db)

        schema_hint = cfg.upstream.schema_hint.model_dump()
        up_res = ingest_upstream_mapping(
            con_work=con,
            upstream_checkout_path=cfg.upstream.checkout_path,
            upstream_db_relpath=cfg.upstream.db_relpath,
            upstream_repo=cfg.upstream.repo,
            upstream_ref=cfg.upstream.ref,
            schema_hint=schema_hint,
        )
        logger.log("upstream.ingested", **up_res)

        sh_hash, sh_n = ingest_shipping_bands(
            con, cfg.tables.shipping_normal_txt, service_level="NORMAL", dest_country="PT"
        )
        logger.log(
            "shipping.ingested",
            service_level="NORMAL",
            bands=sh_n,
            source_hash=sh_hash,
            path=cfg.tables.shipping_normal_txt,
        )

        sh2_hash, sh2_n = ingest_shipping_bands(
            con, cfg.tables.shipping_registered_txt, service_level="REGISTERED", dest_country="PT"
        )
        logger.log(
            "shipping.ingested",
            service_level="REGISTERED",
            bands=sh2_n,
            source_hash=sh2_hash,
            path=cfg.tables.shipping_registered_txt,
        )

        rr_hash, rr_rules = ingest_rarity_rules(con, cfg.tables.rarity_rules_txt)
        logger.log(
            "rarity_rules.ingested",
            rules_hash=rr_hash,
            parsed_rules=rr_rules,
            path=cfg.tables.rarity_rules_txt,
        )

        cfg_hash = sha256_json(cfg.raw)
        manifest = RunManifest(
            run_id=run_id,
            slice_id=slice_id,
            created_ts=utc_now_iso(),
            config_hash=cfg_hash,
            upstream_repo=cfg.upstream.repo,
            upstream_ref=cfg.upstream.ref,
            upstream_commit_sha=up_res["upstream_commit_sha"],
            upstream_db_relpath=cfg.upstream.db_relpath,
            upstream_db_sha256=up_res["upstream_db_sha256"],
            notes="M0 bootstrap",
        )
        manifest_path = write_manifest(manifest, cfg.paths.manifests_dir)

        con.execute(
            "INSERT OR REPLACE INTO run_manifest(run_id, slice_id, manifest_json, created_ts) VALUES(?,?,?,?);",
            (run_id, slice_id, Path(manifest_path).read_text(encoding="utf-8"), utc_now_iso()),
        )
        con.commit()
        con.close()
        logger.log("manifest.written", path=str(manifest_path))

        def cmd_normalize_input(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)

    run_id = args.run_id or make_run_id()
    slice_id = args.slice_id

    lock = RepoLock(cfg.paths.lockfile, ttl_seconds=args.lock_ttl_seconds, wait_seconds=args.lock_wait_seconds)
    logger = JsonlLogger(cfg.paths.logs_dir, run_id=run_id, slice_id=slice_id)

    try:
        lock.acquire()
        logger.log("normalize.start", config_path=args.config, input_path=args.input)

        con = connect(cfg.paths.sqlite_db)
        migrate(con)
        logger.log("db.migrated", db_path=cfg.paths.sqlite_db)

        raw = parse_search_xml(args.input)
        logger.log("normalize.parsed", raw_items=len(raw))

        requested = aggregate_requested(raw)
        logger.log("normalize.aggregated", requested_items=len(requested))

        normalized, stats, total_weight_mg = normalize_items(con, requested)
        norm_hash = normalized_items_hash(normalized)

        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        normalized_path = out_dir / f"normalized_items.{run_id}.json"
        normalized_path.write_text(json.dumps([x.__dict__ for x in normalized], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        input_sha = sha256_file(args.input)
        norm_sha = sha256_file(str(normalized_path))

        # Input manifest separado (para consumo do solver)
        input_manifest = {
            "run_id": run_id,
            "slice_id": slice_id,
            "created_ts": utc_now_iso(),
            "input_path": args.input,
            "input_sha256": input_sha,
            "normalized_items_path": str(normalized_path),
            "normalized_items_sha256": norm_sha,
            "normalized_items_hash": norm_hash,
            "stats": stats,
            "total_weight_mg": total_weight_mg,
        }
        input_manifest["manifest_hash"] = sha256_json(input_manifest)

        input_manifest_path = out_dir / f"input_manifest.{run_id}.json"
        input_manifest_path.write_text(json.dumps(input_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        # incluir no RunManifest para auditoria unificada
        cfg_hash = sha256_json(cfg.raw)

        # recuperar upstream info do último manifest (já ingerido no M0)
        up = con.execute("SELECT manifest_json FROM run_manifest ORDER BY created_ts DESC LIMIT 1;").fetchone()
        upstream_commit_sha = cfg.upstream.ref
        upstream_db_sha256 = ""
        if up is not None:
            try:
                last = json.loads(up[0])
                upstream_commit_sha = last.get("upstream_commit_sha", upstream_commit_sha)
                upstream_db_sha256 = last.get("upstream_db_sha256", upstream_db_sha256)
            except Exception:
                pass

        manifest = RunManifest(
            run_id=run_id,
            slice_id=slice_id,
            created_ts=utc_now_iso(),
            config_hash=cfg_hash,
            upstream_repo=cfg.upstream.repo,
            upstream_ref=cfg.upstream.ref,
            upstream_commit_sha=upstream_commit_sha,
            upstream_db_relpath=cfg.upstream.db_relpath,
            upstream_db_sha256=upstream_db_sha256,
            notes="M1 normalize-input",
            input_path=args.input,
            input_sha256=input_sha,
            input_items_hash=norm_hash,
            normalized_items_path=str(normalized_path),
            normalized_items_sha256=norm_sha,
            missing_mapping_count=stats.get("missing_boid", 0),
            total_weight_mg=total_weight_mg,
        )
        manifest_path = write_manifest(manifest, cfg.paths.manifests_dir)
        con.execute(
            "INSERT OR REPLACE INTO run_manifest(run_id, slice_id, manifest_json, created_ts) VALUES(?,?,?,?);",
            (run_id, slice_id, Path(manifest_path).read_text(encoding="utf-8"), utc_now_iso()),
        )
        con.commit()
        con.close()

        logger.log("normalize.done", **stats, total_weight_mg=total_weight_mg, output_dir=str(out_dir))
        logger.log("manifest.written", path=str(manifest_path))
        logger.log("input_manifest.written", path=str(input_manifest_path))
        return 0

    finally:
        lock.release()

        # -------------------------
        # Git sync (optional)
        # -------------------------
        if cfg.github_sync.enabled:
            do_pull = cfg.github_sync.pull and not args.no_pull
            do_commit = cfg.github_sync.commit and not args.no_commit
            do_push = cfg.github_sync.push and not args.no_push

            if not (do_pull or do_commit or do_push):
                logger.log("git.sync.skipped", reason="all git operations disabled by flags")
            else:
                msg = cfg.github_sync.commit_message_template.format(
                    action="bootstrap",
                    run_id=run_id,
                    slice_id=slice_id,
                    upstream_commit_sha=up_res["upstream_commit_sha"],
                )
                res = sync_repo(
                    repo_root=repo_root,
                    do_pull=do_pull,
                    do_commit=do_commit,
                    do_push=do_push,
                    commit_message=msg,
                    user_name=cfg.github_sync.git_user_name,
                    user_email=cfg.github_sync.git_user_email,
                    fail_safe_on_push_conflict=cfg.github_sync.fail_safe_on_push_conflict,
                )
                logger.log("git.sync", did_commit=res.did_commit, did_push=res.did_push, status=res.status)

        logger.log("bootstrap.done", run_id=run_id, slice_id=slice_id)
        return 0

    except Exception as e:
        logger.log("bootstrap.error", exc_type=type(e).__name__, error=str(e))
        raise

    finally:
        # Liberta lock sem rebentar caso a implementação não tenha release()
        rel = getattr(lock, "release", None)
        if callable(rel):
            try:
                rel()
            except Exception:
                # Não mascara erros do bootstrap
                pass


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="brickovery")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bootstrap", help="M0 bootstrap")
    b.add_argument("--config", required=True)
    b.add_argument("--run-id", default=None)
    b.add_argument("--slice-id", default="0")
    b.add_argument("--no-pull", action="store_true")
    b.add_argument("--no-commit", action="store_true")
    b.add_argument("--no-push", action="store_true")
    b.add_argument("--lock-ttl-seconds", type=int, default=3600)
    b.add_argument("--lock-wait-seconds", type=int, default=0)
    b.set_defaults(func=cmd_bootstrap)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
