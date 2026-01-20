from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import List, Tuple, Optional
import sqlite3

from ..io.hashing import sha256_file
from ..io.timeutil import utc_now_iso


@dataclass
class Band:
    min_mg: int
    max_mg: int
    price_eur: float


_LINE_RE = re.compile(
    r"(?P<min>\d+(?:[.,]\d+)?)\s*g?\s*-\s*(?P<max>\d+(?:[.,]\d+)?)\s*g?\s*=\s*(?:EUR|€)\s*(?P<price>\d+(?:[.,]\d+)?)",
    re.IGNORECASE,
)


def _to_mg(s: str) -> int:
    s = s.strip().replace(",", ".")
    val_g = float(s)
    return int(round(val_g * 1000.0))


def _to_eur(s: str) -> float:
    return float(s.strip().replace(",", ".").replace("€", ""))


def parse_bands(text: str) -> List[Band]:
    bands: List[Band] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = _LINE_RE.search(line)
        if not m:
            continue
        min_mg = _to_mg(m.group("min"))
        max_mg = _to_mg(m.group("max"))
        price = _to_eur(m.group("price"))
        bands.append(Band(min_mg=min_mg, max_mg=max_mg, price_eur=price))

    bands.sort(key=lambda b: (b.min_mg, b.max_mg))

    fixed: List[Band] = []
    last_max: Optional[int] = None
    for b in bands:
        if last_max is not None and b.min_mg <= last_max:
            b = Band(min_mg=last_max + 1, max_mg=b.max_mg, price_eur=b.price_eur)
        if b.max_mg < b.min_mg:
            continue
        fixed.append(b)
        last_max = b.max_mg
    return fixed


def ingest_shipping_bands(con: sqlite3.Connection, txt_path: str, service_level: str, dest_country: str = "PT") -> Tuple[str, int]:
    p = Path(txt_path)
    if not p.exists():
        raise FileNotFoundError(f"Shipping file not found: {txt_path}")

    source_hash = sha256_file(str(p))
    raw = p.read_text(encoding="utf-8", errors="replace")
    bands = parse_bands(raw)
    if not bands:
        raise ValueError(f"No shipping bands parsed from: {txt_path}")

    ts = utc_now_iso()
    con.execute("DELETE FROM shipping_band WHERE service_level=? AND dest_country=?;", (service_level, dest_country))
    for b in bands:
        con.execute(
            "INSERT OR REPLACE INTO shipping_band(service_level, dest_country, weight_min_mg, weight_max_mg, price_eur, source_hash, ingested_ts) VALUES(?,?,?,?,?,?,?);",
            (service_level, dest_country, b.min_mg, b.max_mg, b.price_eur, source_hash, ts),
        )
    con.commit()
    return source_hash, len(bands)
