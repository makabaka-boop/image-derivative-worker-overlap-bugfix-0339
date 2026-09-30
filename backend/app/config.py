from __future__ import annotations

import os
from pathlib import Path


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "/data"))


DATA_DIR: Path = _data_dir()
ORIGINALS_DIR: Path = DATA_DIR / "originals"
DERIVS_DIR: Path = DATA_DIR / "derivatives"
TMP_DIR: Path = DATA_DIR / "tmp"
DB_PATH: Path = DATA_DIR / "app.db"

MAX_UPLOAD_BYTES = 4 * 1024 * 1024  # 4 MiB
ALLOWED_CONTENT_TYPES = {"image/png", "image/jpeg"}
ALLOWED_FORMATS = ("PNG", "JPEG")
SIZES = (256, 512)
WEBP_QUALITY = 85

# 测试钩子：允许请求设置人工处理延迟，用于确定性地制造取消/完成竞态。
TEST_HOOKS_ENABLED = os.environ.get("TEST_HOOKS", "0") == "1"
MAX_TEST_DELAY_MS = 10_000

WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "2"))
WORKER_POLL_INTERVAL = float(os.environ.get("WORKER_POLL_INTERVAL", "0.2"))
