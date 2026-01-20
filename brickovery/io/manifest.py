from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from pathlib import Path
import json

from .hashing import sha256_json


@dataclass
class RunManifest:
    run_id: str
    slice_id: str
    created_ts: str
    config_hash: str
    upstream_repo: str
    upstream_ref: str
    upstream_commit_sha: str
    upstream_db_relpath: str
    upstream_db_sha256: str
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["manifest_hash"] = sha256_json(d)
        return d


def write_manifest(manifest: RunManifest, manifests_dir: str) -> Path:
    out_dir = Path(manifests_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"run_manifest.{manifest.run_id}.{manifest.slice_id}.json"
    path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    latest = out_dir / "run_manifest.latest.json"
    latest.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return path
