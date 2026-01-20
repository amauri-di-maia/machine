from __future__ import annotations

from pathlib import Path
import json
import time
from typing import Optional, Dict, Any
from ..io.timeutil import utc_now_iso


class RepoLockError(RuntimeError):
    pass


class RepoLock:
    def __init__(self, lockfile: str, ttl_seconds: int = 3600):
        self.lockfile = Path(lockfile)
        self.ttl_seconds = ttl_seconds

    def _read(self) -> Optional[Dict[str, Any]]:
        if not self.lockfile.exists():
            return None
        try:
            return json.loads(self.lockfile.read_text(encoding="utf-8"))
        except Exception:
            return None

    def acquire(self, owner_run_id: str, owner_info: Optional[Dict[str, Any]] = None, wait_seconds: int = 0) -> None:
        self.lockfile.parent.mkdir(parents=True, exist_ok=True)

        start = time.time()
        while True:
            cur = self._read()
            now = time.time()
            if cur is None:
                break
            created = cur.get("created_epoch", 0)
            if now - created > self.ttl_seconds:
                break
            if wait_seconds <= 0 or (now - start) > wait_seconds:
                raise RepoLockError(f"Repo lock is held and not stale: {self.lockfile}")
            time.sleep(2)

        payload = {
            "owner_run_id": owner_run_id,
            "created_ts": utc_now_iso(),
            "created_epoch": time.time(),
        }
        if owner_info:
            payload.update(owner_info)
        self.lockfile.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def release(self) -> None:
        try:
            self.lockfile.unlink(missing_ok=True)
        except Exception:
            pass
