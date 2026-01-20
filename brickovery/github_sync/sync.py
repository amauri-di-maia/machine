from __future__ import annotations

from dataclasses import dataclass
from .git import ensure_identity, pull_rebase, status_porcelain, add_all, commit as git_commit, push as git_push


@dataclass
class GitSyncResult:
    did_commit: bool
    did_push: bool
    status: str


def sync_repo(
    repo_root: str,
    do_pull: bool,
    do_commit: bool,
    do_push: bool,
    commit_message: str,
    user_name: str,
    user_email: str,
    fail_safe_on_push_conflict: bool = True,
) -> GitSyncResult:
    ensure_identity(repo_root, user_name, user_email)

    if do_pull:
        pull_rebase(repo_root)

    st = status_porcelain(repo_root)
    if not st:
        return GitSyncResult(did_commit=False, did_push=False, status="clean")

    if do_commit:
        add_all(repo_root)
        git_commit(repo_root, commit_message)
    else:
        return GitSyncResult(did_commit=False, did_push=False, status="dirty_no_commit")

    if do_push:
        try:
            git_push(repo_root)
            return GitSyncResult(did_commit=True, did_push=True, status="pushed")
        except Exception:
            if not fail_safe_on_push_conflict:
                raise
            pull_rebase(repo_root)
            git_push(repo_root)
            return GitSyncResult(did_commit=True, did_push=True, status="pushed_after_rebase")

    return GitSyncResult(did_commit=True, did_push=False, status="committed_no_push")
