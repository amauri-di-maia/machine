from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1


def connect(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con


def migrate(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS schema_version(
        version INTEGER NOT NULL,
        applied_ts TEXT NOT NULL
    );
    """)
    cur = con.execute("SELECT COUNT(*) FROM schema_version;")
    n = cur.fetchone()[0]
    if n == 0:
        con.execute("INSERT INTO schema_version(version, applied_ts) VALUES(0, datetime('now'));")

    con.execute("""
    CREATE TABLE IF NOT EXISTS upstream_state(
        id INTEGER PRIMARY KEY CHECK (id = 1),
        upstream_repo TEXT NOT NULL,
        upstream_ref TEXT NOT NULL,
        upstream_commit_sha TEXT NOT NULL,
        upstream_db_relpath TEXT NOT NULL,
        upstream_db_sha256 TEXT NOT NULL,
        ingested_ts TEXT NOT NULL
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS up_mapping_mirror(
        bl_itemtype TEXT NOT NULL,
        bl_part_id TEXT NOT NULL,
        bl_color_id INTEGER,
        bo_boid INTEGER,
        weight_mg INTEGER,
        source TEXT NOT NULL,
        row_hash TEXT NOT NULL,
        ingested_ts TEXT NOT NULL,
        PRIMARY KEY (bl_itemtype, bl_part_id, bl_color_id)
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_up_mapping_boid ON up_mapping_mirror(bo_boid);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS mapping_overrides(
        bl_itemtype TEXT NOT NULL,
        bl_part_id TEXT NOT NULL,
        bl_color_id INTEGER,
        bo_boid INTEGER,
        status TEXT NOT NULL,
        reason TEXT,
        created_ts TEXT NOT NULL,
        updated_ts TEXT NOT NULL,
        PRIMARY KEY (bl_itemtype, bl_part_id, bl_color_id)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS shipping_band(
        service_level TEXT NOT NULL,
        dest_country TEXT NOT NULL,
        weight_min_mg INTEGER NOT NULL,
        weight_max_mg INTEGER NOT NULL,
        price_eur REAL NOT NULL,
        source_hash TEXT NOT NULL,
        ingested_ts TEXT NOT NULL,
        PRIMARY KEY (service_level, dest_country, weight_min_mg, weight_max_mg)
    );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_shipping_band_level ON shipping_band(service_level, dest_country);")

    con.execute("""
    CREATE TABLE IF NOT EXISTS rarity_rules(
        rules_hash TEXT PRIMARY KEY,
        rules_json TEXT NOT NULL,
        raw_text TEXT NOT NULL,
        ingested_ts TEXT NOT NULL
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS run_manifest(
        run_id TEXT NOT NULL,
        slice_id TEXT NOT NULL,
        manifest_json TEXT NOT NULL,
        created_ts TEXT NOT NULL,
        PRIMARY KEY (run_id, slice_id)
    );
    """)

    con.execute("DELETE FROM schema_version;")
    con.execute("INSERT INTO schema_version(version, applied_ts) VALUES(?, datetime('now'));", (SCHEMA_VERSION,))
    con.commit()
