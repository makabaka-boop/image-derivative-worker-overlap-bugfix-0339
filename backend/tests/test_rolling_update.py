"""滚动更新：新 worker 启动时旧 worker 仍在处理作业。

旧实现会在新进程启动时无条件清空 tmp/ 并把所有 processing 退回 pending，
导致同一作业被两个 worker 重复处理、staging 文件被对方删掉、发布失败。
本用例在 verify 容器内起一套本地 uvicorn + 两个 worker 世代：

  1) 申请一个长延迟作业，等 worker-1 进入 processing 并写出 staging；
  2) 再启动 worker-2（模拟滚动更新拉起新进程），worker-1 不杀；
  3) 作业必须恰好由一个 worker 正常发布，两尺寸齐全、可下载，
     tmp 中无残留；全程不出现 failed。
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import httpx
import pytest

from conftest import make_png, submit, upload

APP_DIR = Path(os.environ.get("APP_DIR", "/app"))


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_health(base: str, timeout: float = 25.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/api/health", timeout=2) as r:
                if r.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.3)
    raise RuntimeError(f"本地 API 未就绪: {base}")


def _spawn_worker(env, log_path: Path) -> subprocess.Popen:
    log = open(log_path, "w")
    return subprocess.Popen(
        [sys.executable, "-m", "app.worker"],
        cwd=str(APP_DIR),
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )


def test_rolling_update_does_not_duplicate_or_clobber(tmp_path):
    if not APP_DIR.exists():
        pytest.skip("恢复测试需要容器内 /app 目录")

    data_dir = tmp_path / "rolling-data"
    port = _free_port()
    # 租约要明显长于作业本身：worker-1 在整个作业期间始终是活租约持有者。
    env = {
        **os.environ,
        "DATA_DIR": str(data_dir),
        "TEST_HOOKS": "1",
        "WORKER_CONCURRENCY": "1",
        "LEASE_TTL_SECONDS": "30",
        "LEASE_HEARTBEAT_SECONDS": "1",
        "PYTHONPATH": str(APP_DIR),
    }

    popens: list[subprocess.Popen] = []
    base = f"http://127.0.0.1:{port}"
    api_log = open(tmp_path / "api.log", "w")
    try:
        api = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(APP_DIR), env=env, stdout=api_log,
            stderr=subprocess.STDOUT, text=True,
        )
        popens.append(api)
        _wait_health(base)

        worker1 = _spawn_worker(env, tmp_path / "worker1.log")
        popens.append(worker1)

        with httpx.Client(base_url=base, timeout=30) as client:
            image = upload(client, make_png(900, 600)).json()["image"]
            r = submit(client, image["id"],
                       {"x": 50, "y": 50, "w": 500, "h": 400}, delay_ms=2500)
            job_id = r.json()["job"]["id"]

        tmp_dir = data_dir / "tmp"
        deadline = time.time() + 10
        while time.time() < deadline:
            if list(tmp_dir.glob("*.staging-*")):
                break
            time.sleep(0.1)
        assert list(tmp_dir.glob("*.staging-*")), "worker-1 应已写出 staging"

        # 滚动更新：新世代 worker 启动，旧 worker 继续存活。
        worker2 = _spawn_worker(env, tmp_path / "worker2.log")
        popens.append(worker2)

        with httpx.Client(base_url=base, timeout=30) as client:
            final = None
            deadline = time.time() + 20
            while time.time() < deadline:
                j = client.get(f"/api/jobs/{job_id}").json()
                if j["status"] in ("published", "cancelled", "failed",
                                   "superseded"):
                    final = j
                    break
                time.sleep(0.1)
            assert final is not None, "作业未到终态"
            assert final["status"] == "published", final
            sizes = sorted(d["size"] for d in final["version"]["derivatives"])
            assert sizes == [256, 512]
            for d in final["version"]["derivatives"]:
                rr = client.get(d["url"])
                assert rr.status_code == 200
                assert rr.content[8:12] == b"WEBP"

            state = client.get(f"/api/images/{image['id']}").json()
            assert state["current_version"]["id"] == final["version"]["id"]

        # 只有一个版本、一条终态作业；不允许出现 failed（重复处理/文件被删的症状）。
        with httpx.Client(base_url=base, timeout=30) as client:
            jobs = []
            deadline = time.time() + 3
            # 没有列表接口，直接查库文件：用 sqlite3 标准库只读校验。
            import sqlite3
            db_path = data_dir / "app.db"
            deadline = time.time() + 5
            while time.time() < deadline:
                con = sqlite3.connect(str(db_path))
                try:
                    rows = con.execute(
                        "SELECT status, COUNT(*) FROM jobs GROUP BY status"
                    ).fetchall()
                    n_versions = con.execute(
                        "SELECT COUNT(*) FROM versions"
                    ).fetchone()[0]
                finally:
                    con.close()
                jobs = rows
                if any(s == "published" for s, _ in rows):
                    break
                time.sleep(0.2)
            statuses = dict(jobs)
            assert statuses.get("published") == 1, statuses
            assert "failed" not in statuses, statuses
            assert n_versions == 1, f"重复处理产生多个版本: {n_versions}"

        leftovers = list(tmp_dir.glob("*.staging-*"))
        assert not leftovers, f"残留临时文件: {leftovers}"

        # 新 worker 启动日志必须显示"回收过期租约作业 0 个"（活作业未被抢）。
        time.sleep(0.5)
        log_text = (tmp_path / "worker2.log").read_text()
        assert "回收过期租约作业 0 个" in log_text, log_text

    finally:
        for p in popens:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    p.kill()
