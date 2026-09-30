from __future__ import annotations

import os
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


def save_original_atomic(dst: Path, data: bytes) -> None:
    """先写临时文件再原子替换；同一内容并发上传时复用已有文件。"""
    if dst.exists():
        return
    tmp = tmp_path(f"orig-{os.getpid()}-{dst.name}")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, dst)


def write_staged_deriv(tmp: Path, webp_bytes: bytes) -> None:
    tmp.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp, "wb") as f:
        f.write(webp_bytes)
        f.flush()
        os.fsync(f.fileno())


def publish_deriv(tmp: Path, dst: Path) -> None:
    # 内容寻址：目标已存在说明此前已发布过（或崩溃后重跑），替换即可。
    os.replace(tmp, dst)


def cleanup_tmp() -> None:
    """崩溃重启后清理全部临时文件。"""
    if not config.TMP_DIR.exists():
        return
    for p in config.TMP_DIR.iterdir():
        try:
            if p.is_file() or p.is_symlink():
                p.unlink()
        except FileNotFoundError:
            pass
