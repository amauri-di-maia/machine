from __future__ import annotations

from pathlib import Path
import json
from typing import Any, Dict
from ..io.timeutil import utc_now_iso


class JsonlLogger:
    def __init__(self, logs_dir: str, run_id: str, slice_id: str):
        self.run_id = run_id
        self.slice_id = slice_id
        out_dir = Path(logs_dir) / f"run-{run_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        self.path = out_dir / f"slice-{slice_id}.jsonl"

    def log(self, event: str, **fields: Any) -> None:
        rec: Dict[str, Any] = {
            "ts": utc_now_iso(),
            "run_id": self.run_id,
            "slice_id": self.slice_id,
            "event": event,
        }
        rec.update(fields)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
