from __future__ import annotations

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Any


class PathsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    repo_root: str = "."
    sqlite_db: str = "data base/brickovery.db"
    lockfile: str = "data base/locks/brickovery.lock"
    manifests_dir: str = "data base/manifests"
    logs_dir: str = "data base/logs"


class TablesConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    shipping_normal_txt: str = "data/tables/shipping_normal.txt"
    shipping_registered_txt: str = "data/tables/shipping_registered.txt"
    rarity_rules_txt: str = "data/tables/rarity_rules.txt"


class UpstreamSchemaHint(BaseModel):
    model_config = ConfigDict(extra="allow")
    mapping_table: Optional[str] = None
    bl_part_col: Optional[str] = None
    bl_color_col: Optional[str] = None
    bl_itemtype_col: Optional[str] = None
    bo_boid_col: Optional[str] = None
    weight_g_col: Optional[str] = None
    weight_table: Optional[str] = None
    weight_part_col: Optional[str] = None
    weight_itemtype_col: Optional[str] = None
    weight_value_col: Optional[str] = None


class UpstreamConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    repo: str
    ref: str
    db_relpath: str
    checkout_path: str = "upstream/amauri-repo"
    token_env: str = "UPSTREAM_REPO_TOKEN"
    schema_hint: UpstreamSchemaHint = Field(default_factory=UpstreamSchemaHint)


class GithubSyncConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    enabled: bool = True
    pull: bool = True
    commit: bool = True
    push: bool = True
    fail_safe_on_push_conflict: bool = True
    commit_message_template: str = "brickovery: {action} (run_id={run_id}, slice_id={slice_id}, upstream_sha={upstream_commit_sha})"
    git_user_name: str = "github-actions[bot]"
    git_user_email: str = "41898282+github-actions[bot]@users.noreply.github.com"


class RunConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    seed: int = 1337
    timezone: str = "Europe/Lisbon"


class BrickoveryConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    version: int = 1
    paths: PathsConfig = Field(default_factory=PathsConfig)
    tables: TablesConfig = Field(default_factory=TablesConfig)
    upstream: UpstreamConfig
    github_sync: GithubSyncConfig = Field(default_factory=GithubSyncConfig)
    run: RunConfig = Field(default_factory=RunConfig)

    # Keep raw config for hashing/audit
    raw: Dict[str, Any] = Field(default_factory=dict, exclude=True)
