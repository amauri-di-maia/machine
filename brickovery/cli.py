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

        sh_hash, sh_n = ingest_shipping_bands(con, cfg.tables.shipping_normal_txt, service_level="NORMAL", dest_country="PT")
        logger.log("shipping.ingested", service_level="NORMAL", bands=sh_n, source_hash=sh_hash, path=cfg.tables.shipping_normal_txt)

        sh2_hash, sh2_n = ingest_shipping_bands(con, cfg.tables.shipping_registered_txt, service_level="REGISTERED", dest_country="PT")
        logger.log("shipping.ingested", service_level="REGISTERED", bands=sh2_n, source_hash=sh2_hash, path=cfg.tables.shipping_registered_txt)

        rr_hash, rr_rules = ingest_rarity_rules(con, cfg.tables.rarity_rules_txt)
        logger.log("rarity_rules.ingested", rules_hash=rr_hash, parsed_rules=rr_rules, path=cfg.tables.rarity_rules_txt)

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

        if cfg.github_sync.enabled:
            msg = cfg.github_sync.commit_message_template.format(
                action="bootstrap",
                run_id=run_id,
                slice_id=slice_id,
                upstream_commit_sha=up_res["upstream_commit_sha"],
            )
            res = sync_repo(
                repo_root=repo_root,
                do_pull=cfg.github_sync.pull and not args.no_pull,
                do_commit=cfg.github_sync.commit and not args.no_commit,
                do_push=cfg.github_sync.push and not args.no_push,
                commit_message=msg,
                user_name=cfg.github_sync.git_user_name,
                user_email=cfg.github_sync.git_user_email,
                fail_safe_on_push_conflict=cfg.github_sync.fail_safe_on_push_conflict,
            )
            logger.log("git.sync", did_commit=res.did_commit, did_push=res.did_push, status=res.status)

        logger.log("bootstrap.done")
        return 0
    finally:
        lock.release()


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
