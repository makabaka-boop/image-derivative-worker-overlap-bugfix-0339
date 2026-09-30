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
MAX_TEST_DELAY_MS = 60_000

WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "2"))
WORKER_POLL_INTERVAL = float(os.environ.get("WORKER_POLL_INTERVAL", "0.2"))

# 作业租约：worker 认领后持有的最长时长，心跳线程按 TTL/3 续租。
# 只有租约过期的 processing 作业才允许被其他 worker 接管——
# 滚动更新时新旧进程并存，绝不能把仍在续租的活作业重新入队。
WORKER_LEASE_TTL_SECONDS = float(os.environ.get("WORKER_LEASE_TTL_SECONDS", "30"))
WORKER_HEARTBEAT_INTERVAL_SECONDS = float(
    os.environ.get("WORKER_HEARTBEAT_INTERVAL_SECONDS", "")
    or WORKER_LEASE_TTL_SECONDS / 3
)
# 优雅停机等待在途作业退出的上限（与 compose stop_grace_period 对齐）。
WORKER_SHUTDOWN_GRACE_SECONDS = float(
    os.environ.get("WORKER_SHUTDOWN_GRACE_SECONDS", "8")
)
