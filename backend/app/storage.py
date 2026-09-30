from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Iterable

from . import config

SUFFIX = {"PNG": ".png", "JPEG": ".jpg", "WEBP": ".webp"}


def init_storage() -> None:
    for d in (config.ORIGINALS_DIR, config.DERIVS_DIR, config.TMP_DIR):
        d.mkdir(parents=True, exist_ok=True)


def original_path(sha256_hex: str, fmt: str) -> Path:
    return config.ORIGINALS_DIR / f"{sha256_hex}{SUFFIX[fmt]}"


def deriv_path(key: str) -> Path:
    return config.DERIVS_DIR / f"{key}.webp"


def tmp_path(name: str) -> Path:
    return config.TMP_DIR / name


def _new_tmp(prefix: str) -> Path:
    """带随机 token 的临时文件：pid 在滚动更新的新旧容器间可能重复，
    只用 pid 会让同一内容的并行上传/重试写同一个文件互相截断。"""
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    return config.TMP_DIR / f"{prefix}-{os.getpid()}-{uuid.uuid4().hex[:12]}"


def save_original_atomic(dst: Path, data: bytes) -> None:
    """先写临时文件再原子替换；同一内容并发上传时复用已有文件。"""
    if dst.exists():
        return
    tmp = _new_tmp(f"orig-{dst.name}")
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, dst)
    finally:
        # 并发上传同内容时，另一个请求可能已完成替换，本请求的临时文件残留在此。
        tmp.unlink(missing_ok=True)


def staging_tmp(job_id: str, attempt: str, key: str, target: int) -> Path:
    """某次渲染尝试独占的 staging 文件名，含作业 id（便于清扫）与随机 token
    （同一作业被新旧 worker 重试时也绝不撞名）。"""
    config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    return config.TMP_DIR / f"{key}.staging-{job_id}-{target}-{attempt}"


def write_staged_deriv(tmp: Path, webp_bytes: bytes) -> None:
    tmp.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp, "wb") as f:
        f.write(webp_bytes)
        f.flush()
        os.fsync(f.fileno())


def publish_deriv(tmp: Path, dst: Path) -> None:
    # 内容寻址：目标已存在说明此前已发布过（或崩溃后重跑），替换即可。
    os.replace(tmp, dst)


def discard_job_attempts(job_id: str, keep: Path | None = None) -> None:
    """删掉某作业此前尝试留下的 staging 文件（保留本次尝试的 keep）。

    作业被重新认领后、新一次渲染开始前调用：旧进程的残留 staging 只可能
    属于这次重试，确定性删除，避免最终产物检查错把旧文件当成新成果。
    """
    if not config.TMP_DIR.exists():
        return
    for p in config.TMP_DIR.glob(f"*.staging-{job_id}-*"):
        if keep is not None and p == keep:
            continue
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass


def sweep_tmp(terminal_jobs: Iterable[str], now_ts: datetime,
             *, stale_after_seconds: float | None = None) -> None:
    """安全清扫：绝不整目录清空（滚动更新时其他存活进程仍在用 tmp）。

    staging 文件名带作业 id，按作业状态决定：
      - 作业已终态        → 一定是残留，删除；
      - 作业在途/租约过期 → 正在使用或即将重试，跳过（重试开始时由
                            discard_job_attempts 确定性清理）；
      - 无法解析归属的文件 → 超过 stale_after_seconds 才删除。
    """
    if not config.TMP_DIR.exists():
        return
    terminal = set(terminal_jobs)
    stale_age = (stale_after_seconds if stale_after_seconds is not None
                 else config.STALE_TMP_SECONDS)
    for p in config.TMP_DIR.iterdir():
        if not (p.is_file() or p.is_symlink()):
            continue
        job_id = _staging_job_id(p.name)
        if job_id is not None:
            if job_id in terminal:
                _unlink_quiet(p)
            # 在途/租约过期但未终态的作业不碰：重试开始时由
            # discard_job_attempts 确定性清理旧尝试文件。
            continue
        try:
            age = now_ts.timestamp() - p.stat().st_mtime
        except (FileNotFoundError, OSError):
            continue
        if age >= stale_age:
            _unlink_quiet(p)


def _staging_job_id(name: str) -> str | None:
    # 形如 <key>.staging-<job_id>-<size>-<attempt>
    if ".staging-" not in name:
        return None
    rest = name.split(".staging-", 1)[1]
    parts = rest.split("-")
    return parts[0] if parts and parts[0] else None


def _unlink_quiet(p: Path) -> None:
    try:
        p.unlink(missing_ok=True)
    except OSError:
        pass
