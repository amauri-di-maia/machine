from __future__ import annotations

import datetime


def utc_now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def make_run_id() -> str:
    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
