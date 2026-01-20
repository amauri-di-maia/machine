from __future__ import annotations

import subprocess
from typing import List


def run_git(args: List[str], cwd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=check)


def ensure_identity(cwd: str, user_name: str, user_email: str) -> None:
    run_git(["config", "user.name", user_name], cwd=cwd, check=True)
    run_git(["config", "user.email", user_email], cwd=cwd, check=True)


def pull_rebase(cwd: str) -> None:
    run_git(["pull", "--rebase"], cwd=cwd, check=True)


def status_porcelain(cwd: str) -> str:
    cp = run_git(["status", "--porcelain"], cwd=cwd, check=True)
    return cp.stdout.strip()


def add_all(cwd: str) -> None:
    run_git(["add", "-A"], cwd=cwd, check=True)


def commit(cwd: str, message: str) -> None:
    run_git(["commit", "-m", message], cwd=cwd, check=True)


def push(cwd: str) -> None:
    run_git(["push"], cwd=cwd, check=True)


def head_sha(cwd: str) -> str:
    cp = run_git(["rev-parse", "HEAD"], cwd=cwd, check=True)
    return cp.stdout.strip()
