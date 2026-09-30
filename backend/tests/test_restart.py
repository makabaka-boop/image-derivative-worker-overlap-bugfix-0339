"""worker 崩溃重启恢复：

在 verify 容器内用独立 DATA_DIR 起一套本地 uvicorn + worker 子进程；
在作业渲染中途 SIGKILL 掉 worker，重启后必须：
  1) 清理遗留临时文件；
  2) processing 作业退回 pending；
  3) 重试并最终发布，派生图可下载。
"""
from __future__ import annotations

import os
import signal
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


def _start(env: dict, port: int, popens: list, log_dir: Path):
    api_log = open(log_dir / "api.log", "w")
    worker_log = open(log_dir / "worker.log", "w")
    api = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(APP_DIR),
        env=env,
        stdout=api_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    popens.append(api)
    _wait_health(f"http://127.0.0.1:{port}")
    worker = subprocess.Popen(
        [sys.executable, "-m", "app.worker"],
        cwd=str(APP_DIR),
        env=env,
        stdout=worker_log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    popens.append(worker)
    return api, worker


def _wait_until(path: Path, timeout: float = 10.0) -> Path:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            return path
        time.sleep(0.05)
    raise AssertionError(f"等待文件出现超时: {path}")


def test_worker_crash_restart_recovers(tmp_path):
    if not APP_DIR.exists():
        pytest.skip("恢复测试需要容器内 /app 目录")

    data_dir = tmp_path / "recovery-data"
    port = _free_port()
    env = {
        **os.environ,
        "DATA_DIR": str(data_dir),
        "TEST_HOOKS": "1",
        "WORKER_CONCURRENCY": "1",
        "PYTHONPATH": str(APP_DIR),
    }

    popens: list[subprocess.Popen] = []
    base = f"http://127.0.0.1:{port}"
    try:
        api, worker = _start(env, port, popens, tmp_path)

        with httpx.Client(base_url=base, timeout=30) as client:
            image = upload(client, make_png(900, 600)).json()["image"]
            r = submit(client, image["id"],
                       {"x": 50, "y": 50, "w": 500, "h": 400}, delay_ms=3000)
            job_id = r.json()["job"]["id"]

        tmp_dir = data_dir / "tmp"
        # delay 切片：渲染在第 1 个 ~1s 后发生；等待至少一个临时文件落盘，
        # 此时 worker 正处在三段睡眠/渲染中间。
        _wait_until(tmp_dir, timeout=10)
        # 给一点时间确保第一个 staging 文件写出。
        deadline = time.time() + 5
        staged = []
        while time.time() < deadline:
            staged = list(tmp_dir.glob("*.staging-*"))
            if staged:
                break
            time.sleep(0.1)
        assert staged, "崩溃前应有 staging 临时文件"

        # SIGKILL：模拟崩溃，不给任何优雅退出机会。
        worker.send_signal(signal.SIGKILL)
        worker.wait(timeout=10)
        popens.remove(worker)

        # 重启 worker：启动时必须清理临时文件、重置 processing。
        w2_log = open(tmp_path / "worker2.log", "w")
        worker2 = subprocess.Popen(
            [sys.executable, "-m", "app.worker"],
            cwd=str(APP_DIR),
            env=env,
            stdout=w2_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        popens.append(worker2)

        # 临时文件被启动清理扫掉（重启后 3s 延迟仍会再产生新 staging，
        # 但启动那一刻旧文件必须先被清掉；验证恢复计数与最终状态）。
        with httpx.Client(base_url=base, timeout=30) as client:
            deadline = time.time() + 20
            final = None
            while time.time() < deadline:
                j = client.get(f"/api/jobs/{job_id}").json()
                if j["status"] in ("published", "cancelled", "failed",
                                   "superseded"):
                    final = j
                    break
                time.sleep(0.2)
            assert final is not None, "重启后作业未到终态"
            assert final["status"] == "published", final
            assert final["version"] is not None
            sizes = sorted(d["size"] for d in final["version"]["derivatives"])
            assert sizes == [256, 512]
            for d in final["version"]["derivatives"]:
                rr = client.get(d["url"])
                assert rr.status_code == 200
                assert rr.content[8:12] == b"WEBP"

            state = client.get(f"/api/images/{image['id']}").json()
            assert state["current_version"]["id"] == final["version"]["id"]

        # 收尾后 tmp 中不残留 staging 文件（只允许原子替换后无残留）。
        leftovers = list((data_dir / "tmp").glob("*.staging-*"))
        assert not leftovers, f"残留临时文件: {leftovers}"

    finally:
        for p in popens:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    p.kill()
