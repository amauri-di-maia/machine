from __future__ import annotations

from pathlib import Path
import yaml
from .schema import BrickoveryConfig


def load_config(path: str) -> BrickoveryConfig:
    p = Path(path)
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping/dict. Got: {type(data)}")
    cfg = BrickoveryConfig(**data)
    cfg.raw = data
    return cfg
