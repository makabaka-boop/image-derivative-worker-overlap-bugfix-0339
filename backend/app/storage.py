from __future__ import annotations

import os
import uuid
from pathlib import Path

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


def _unique_tmp(prefix: str, dst_name: str) -> Path:
    """每次写临时文件都用独立名字（pid+uuid）：不同请求/进程即使处理同一
    内容也不会共享路径、互相截断；rename 只在 fsync 完成后发生。"""
    return config.TMP_DIR / f"{prefix}-{os.getpid()}-{uuid.uuid4().hex}-{dst_name}"


def save_original_atomic(dst: Path, data: bytes) -> None:
    """先写独立临时文件再原子替换；同一内容并发上传时复用已有文件。

    每个调用者使用 pid+uuid 的独立临时路径，互不截断；目标已存在则跳过。
    即使“检查存在”与“替换”之间被另一上传者抢先落盘，os.replace 覆盖的
    也是同一 sha256 寻址的相同字节，结果仍然一致。
    """
    if dst.exists():
        return
    tmp = _unique_tmp("orig", dst.name)
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, dst)
        tmp = None
    finally:
        if tmp is not None:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass


def staging_path(key: str, job_id: str, size: int, fence: int) -> Path:
    """staging 名字含作业 + 围栏令牌 + 唯一串：同一作业被不同进程
    （租约过期接管）重跑时使用不同文件，绝不共用、互删。"""
    return config.TMP_DIR / (
        f"{key}.staging-{job_id}-{size}-f{fence}-{uuid.uuid4().hex[:8]}"
    )


def write_staged_deriv(tmp: Path, webp_bytes: bytes) -> None:
    tmp.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp, "wb") as f:
        f.write(webp_bytes)
        f.flush()
        os.fsync(f.fileno())


def publish_deriv(tmp: Path, dst: Path) -> None:
    # 内容寻址：目标已存在说明此前已发布过（或接管后重跑），替换即可。
    os.replace(tmp, dst)


def discard_job_staging(job_id: str) -> None:
    """作业被接管/回收时，清掉上一持有者可能遗留的 staging 文件。

    只清该作业的文件；调用时机保证作业已无活持有者（认领事务刚把它
    易主，下一持有者此时还没开始写自己的 staging）。
    """
    if not config.TMP_DIR.exists():
        return
    for p in config.TMP_DIR.glob(f"*.staging-{job_id}-*"):
        try:
            if p.is_file() or p.is_symlink():
                p.unlink()
        except FileNotFoundError:
            pass
