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

# 作业租约：认领时写入 worker_id + 到期时间，心跳续租。
# 只有租约过期的 processing 作业才会被重新入队，滚动更新时新进程
# 不会抢走仍存活的旧进程手里的作业。
LEASE_TTL_SECONDS = float(os.environ.get("LEASE_TTL_SECONDS", "10"))
LEASE_HEARTBEAT_SECONDS = float(os.environ.get("LEASE_HEARTBEAT_SECONDS", "2"))
# 周期性对账间隔：回收过期租约、清扫已成终态作业的残留临时文件。
RECONCILE_INTERVAL_SECONDS = float(
    os.environ.get("RECONCILE_INTERVAL_SECONDS", "1")
)
# SIGTERM 后等待在途作业自然完成的最长时间（应小于 compose 的
# stop_grace_period=10s，留出进程退出余量）。
SHUTDOWN_GRACE_SECONDS = float(os.environ.get("SHUTDOWN_GRACE_SECONDS", "8"))
# 无法归属（文件名里没有作业 id）的临时文件，超过该年龄才允许清扫。
STALE_TMP_SECONDS = float(os.environ.get("STALE_TMP_SECONDS", "3600"))
