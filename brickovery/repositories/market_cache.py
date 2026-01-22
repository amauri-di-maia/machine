from __future__ import annotations

import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from ..providers.bricklink_html_provider import Offer, CurrentStats


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute("""
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
    """)

    con.execute("""
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
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_market_offers_key ON market_offers(platform, bl_itemtype, bl_part_id, bl_color_id, condition);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_market_offers_snap ON market_offers(snapshot_id);")


def _to_txt_decimal(v: Optional[Decimal]) -> Optional[str]:
    if v is None:
        return None
    return format(v, "f")


def _store_key_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    # BrickLink store URL tem store.asp?sID=... ou similar; guardamos URL como chave mínima determinística.
    return url


def get_last_fetched_ts(
    con: sqlite3.Connection,
    *,
    platform: str,
    bl_itemtype: str,
    bl_part_id: str,
    bl_color_id: Optional[int],
    condition: str,
) -> Optional[str]:
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


def is_fresh(ts: Optional[str], ttl_hours: int) -> bool:
    if not ts:
        return False
    dt = _parse_iso(ts)
    return (datetime.now(timezone.utc) - dt) <= timedelta(hours=ttl_hours)


def upsert_metrics(
    con: sqlite3.Connection,
    *,
    platform: str,
    bl_itemtype: str,
    bl_part_id: str,
    bl_color_id: Optional[int],
    condition: str,
    sold_6m_total_qty: Optional[int],
    sold_6m_avg_price: Optional[Decimal],
    current_min_price: Optional[Decimal],
    current_min_price_is_estimate: Optional[bool],
    current_total_lots: int,
    current_total_qty: int,
    snapshot_id: str,
    fetched_ts: str,
    data_source: str,
    parser_version: str,
) -> None:
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
            platform, bl_itemtype, bl_part_id, bl_color_id, condition,
            sold_6m_total_qty, _to_txt_decimal(sold_6m_avg_price),
            _to_txt_decimal(current_min_price),
            1 if current_min_price_is_estimate else 0 if current_min_price_is_estimate is not None else None,
            int(current_total_lots), int(current_total_qty),
            fetched_ts, snapshot_id, data_source, parser_version,
        ),
    )


def replace_offers(
    con: sqlite3.Connection,
    *,
    platform: str,
    bl_itemtype: str,
    bl_part_id: str,
    bl_color_id: Optional[int],
    condition: str,
    offers: List[Offer],
    snapshot_id: str,
    fetched_ts: str,
    data_source: str,
    parser_version: str,
) -> None:
    con.execute(
        """
        DELETE FROM market_offers
        WHERE platform=? AND bl_itemtype=? AND bl_part_id=? AND
              ((bl_color_id IS NULL AND ? IS NULL) OR (bl_color_id=?)) AND condition=?
        """,
        (platform, bl_itemtype, bl_part_id, bl_color_id, bl_color_id, condition),
    )

    for o in offers:
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
                platform, snapshot_id, bl_itemtype, bl_part_id, bl_color_id, condition,
                _store_key_from_url(o.store_url), o.store_url, o.country_iso2,
                1 if o.ships_to_me else 0 if o.ships_to_me is not None else None,
                int(o.qty), _to_txt_decimal(o.price), 1 if o.is_estimate else 0,
                fetched_ts, data_source, parser_version,
            ),
        )
