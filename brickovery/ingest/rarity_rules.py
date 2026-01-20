from __future__ import annotations

from pathlib import Path
import sqlite3
import json
import re
from typing import Dict, Any, List, Tuple

from ..io.hashing import sha256_file
from ..io.timeutil import utc_now_iso


RARITY_CLASS_TO_SCORE = {
    "COMUM": 0,
    "INCOMUM": 1,
    "RARA": 2,
    "ULTRA RARA": 3,
}


def _norm_class(s: str) -> str:
    s = s.strip().upper()
    s = s.replace("Ú", "U").replace("Ã", "A").replace("Á", "A").replace("Ç", "C")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("ULTRA-RARA", "ULTRA RARA")
    return s


def _parse_weights(text: str) -> Dict[str, float]:
    weights = {}
    for m in re.finditer(r"[-*]\s*(.+?):\s*(\d+(?:[.,]\d+)?)\s*%", text, flags=re.IGNORECASE):
        k = m.group(1).strip().lower()
        v = float(m.group(2).replace(",", ".")) / 100.0
        weights[k] = v
    return weights


def parse_rarity_rules(text: str) -> Dict[str, Any]:
    weights = _parse_weights(text)
    return {
        "parser_version": "rarity_rules_v1",
        "weights": weights,
        "notes": "rules_json is best-effort; raw_text is authoritative. Ensure rarity_rules.txt contains full tables (no ellipses) for full scoring.",
    }


def ingest_rarity_rules(con: sqlite3.Connection, txt_path: str) -> Tuple[str, int]:
    p = Path(txt_path)
    if not p.exists():
        raise FileNotFoundError(f"Rarity rules file not found: {txt_path}")

    rules_hash = sha256_file(str(p))
    raw = p.read_text(encoding="utf-8", errors="replace")
    rules_json = json.dumps(parse_rarity_rules(raw), ensure_ascii=False, indent=2)
    ts = utc_now_iso()

    con.execute(
        "INSERT OR REPLACE INTO rarity_rules(rules_hash, rules_json, raw_text, ingested_ts) VALUES(?,?,?,?);",
        (rules_hash, rules_json, raw, ts),
    )
    con.commit()
    return rules_hash, 0
