"""worker 崩溃/滚动更新恢复：

在 verify 容器内用独立 DATA_DIR 起一套本地 uvicorn + worker 子进程；
在作业渲染中途 SIGKILL/SIGTERM worker，重启后必须：
  1) 旧进程仍持有有效租约时，新进程不得重复认领同一作业；
  2) SIGKILL 崩溃后由租约超时接管，旧 staging 按作业清理，重试并发布；
  3) SIGTERM 优雅停机时旧进程把租约交回 pending，新进程立即续跑；
  4) 接管后旧进程"复活"，其迟到的失败/发布写操作被围栏令牌挡下，
     已发布成果不被翻盘。
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


def _start_one(
    argv: list[str],
    env: dict,
    log_path: Path,
    *,
    new_session: bool = False,
) -> subprocess.Popen:
    """启动子进程并写日志。

    new_session=True 时让子进程自立进程组（setsid）：SIGSTOP/SIGCONT 等
    控制信号只发给单个 worker，绝不波及其它兄弟进程或 pytest 自身。
    """
    log = open(log_path, "w")
    return subprocess.Popen(
        argv,
        cwd=str(APP_DIR),
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=new_session,
    )


def _start(env: dict, port: int, popens: list, log_dir: Path):
    api = _start_one(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port)],
        env, log_dir / "api.log",
    )
    popens.append(api)
    _wait_health(f"http://127.0.0.1:{port}")
    worker = _start_one(
        [sys.executable, "-m", "app.worker"], env, log_dir / "worker.log"
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
        # 崩溃恢复依赖租约超时接管：用短 TTL 让测试在数十秒内完成，
        # 心跳间隔默认 = TTL/3。
        "WORKER_LEASE_TTL_SECONDS": "2",
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

        # 重启 worker：启动时不再清空 tmp/，只回收租约过期的作业；
        # 当前租约已随 SIGKILL 死亡，到点后由认领接管并清掉旧 staging。
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


def _start_worker(
    env: dict, log_path: Path, *, new_session: bool = False
) -> subprocess.Popen:
    return _start_one(
        [sys.executable, "-m", "app.worker"],
        env, log_path, new_session=new_session,
    )


def test_sigterm_during_processing_hands_job_back_immediately(tmp_path):
    """滚动更新走 SIGTERM：旧 worker 应把仍持有的作业交回 pending，
    新 worker 无需等租约超时即可续跑，且最终只产出一个版本、无残留。"""
    if not APP_DIR.exists():
        pytest.skip("恢复测试需要容器内 /app 目录")

    data_dir = tmp_path / "graceful-data"
    port = _free_port()
    env = {
        **os.environ,
        "DATA_DIR": str(data_dir),
        "TEST_HOOKS": "1",
        "WORKER_CONCURRENCY": "1",
        "WORKER_LEASE_TTL_SECONDS": "30",  # 关键：不靠租约超时，靠显式释放
        "WORKER_SHUTDOWN_GRACE_SECONDS": "5",
        "PYTHONPATH": str(APP_DIR),
    }

    popens: list[subprocess.Popen] = []
    base = f"http://127.0.0.1:{port}"
    try:
        api = _start_one(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port)],
            env, tmp_path / "api.log", new_session=True,
        )
        popens.append(api)
        _wait_health(base)
        worker = _start_worker(env, tmp_path / "worker.log", new_session=True)
        popens.append(worker)

        with httpx.Client(base_url=base, timeout=30) as client:
            image = upload(client, make_png(900, 600)).json()["image"]
            r = submit(client, image["id"],
                       {"x": 50, "y": 50, "w": 500, "h": 400}, delay_ms=6000)
            job_id = r.json()["job"]["id"]

            _wait_until(
                data_dir / "tmp", timeout=10
            )  # tmp 目录创建即可，确保 worker 已起来
            # 等作业进入 processing 且心跳已写租约。
            deadline = time.time() + 10
            while time.time() < deadline:
                j = client.get(f"/api/jobs/{job_id}").json()
                if j["status"] == "processing":
                    break
                time.sleep(0.1)
            assert j["status"] == "processing"

            worker.send_signal(signal.SIGTERM)
            worker.wait(timeout=10)
            popens.remove(worker)

            # 停机释放必须很快生效（远短于 30s 租约），证明不是等超时。
            deadline = time.time() + 3
            pending_seen = False
            while time.time() < deadline:
                j = client.get(f"/api/jobs/{job_id}").json()
                if j["status"] == "pending":
                    pending_seen = True
                    break
                if j["status"] in ("published", "failed"):
                    break
                time.sleep(0.05)
            assert pending_seen, f"作业未在停机时交回队列: {j['status']}"

        worker2 = _start_worker(env, tmp_path / "worker2.log", new_session=True)
        popens.append(worker2)

        with httpx.Client(base_url=base, timeout=30) as client:
            final = None
            deadline = time.time() + 15
            while time.time() < deadline:
                j = client.get(f"/api/jobs/{job_id}").json()
                if j["status"] in ("published", "failed", "superseded",
                                   "cancelled"):
                    final = j
                    break
                time.sleep(0.1)
            assert final is not None and final["status"] == "published", final
            sizes = sorted(d["size"] for d in final["version"]["derivatives"])
            assert sizes == [256, 512]

            # 同参数只应有一个版本（没有被两个进程重复处理出两份成果）。
            versions = {
                v["id"] for v in [final["version"]]
            }
            again = submit(client, image["id"],
                           {"x": 50, "y": 50, "w": 500, "h": 400})
            assert again.json()["job"]["version"]["id"] in versions

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


def test_live_lease_blocks_second_worker_and_stale_owner_is_fenced(tmp_path):
    """滚动更新最危险的脑裂场景：

    1) w1 正在处理、租约有效时 w2 启动——w2 绝不能把活作业重新入队/重复处理；
    2) w1 被冻结（模拟旧进程卡死/容器挂起）直到租约过期，w2 才接管并发布；
    3) w1 随后解冻"复活"，它迟到的心跳与发布必须被 worker_id+fence 挡下，
       已发布结果不能被翻盘成 failed/superseded，也不能写坏文件。
    """
    if not APP_DIR.exists():
        pytest.skip("恢复测试需要容器内 /app 目录")

    data_dir = tmp_path / "splitbrain-data"
    port = _free_port()
    # TTL 短：冻结后 ~2s 内可被接管；渲染延迟足够长，保证解冻前 w2 已接管。
    env = {
        **os.environ,
        "DATA_DIR": str(data_dir),
        "TEST_HOOKS": "1",
        "WORKER_CONCURRENCY": "1",
        "WORKER_LEASE_TTL_SECONDS": "2",
        "PYTHONPATH": str(APP_DIR),
    }

    popens: list[subprocess.Popen] = []
    base = f"http://127.0.0.1:{port}"
    # 所有子进程自立进程组：对单个 worker 的 SIGSTOP/SIGCONT/SIGTERM
    # 只影响它自己，绝不波及其它 worker / api / pytest。
    try:
        api = _start_one(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port)],
            env, tmp_path / "api.log", new_session=True,
        )
        popens.append(api)
        _wait_health(base)
        w1 = _start_worker(env, tmp_path / "worker.log", new_session=True)
        popens.append(w1)

        with httpx.Client(base_url=base, timeout=30) as client:
            image = upload(client, make_png(900, 600)).json()["image"]
            r = submit(client, image["id"],
                       {"x": 0, "y": 0, "w": 600, "h": 400}, delay_ms=12000)
            job_id = r.json()["job"]["id"]

            deadline = time.time() + 10
            while time.time() < deadline:
                j = client.get(f"/api/jobs/{job_id}").json()
                if j["status"] == "processing":
                    break
                time.sleep(0.1)
            assert j["status"] == "processing"

        # 两个 worker 并存、w1 租约完全有效：w2 不得抢走作业。
        w2 = _start_worker(env, tmp_path / "worker2.log", new_session=True)
        popens.append(w2)
        time.sleep(3.0)  # > 一个 TTL，但 w1 在持续续租

        with httpx.Client(base_url=base, timeout=30) as client:
            j = client.get(f"/api/jobs/{job_id}").json()
            assert j["status"] == "processing", (
                f"活租约被新进程重复认领: {j['status']}"
            )

        # 冻结旧持有者：心跳停止，租约过期后 w2 才能接管。
        w1.send_signal(signal.SIGSTOP)
        try:
            with httpx.Client(base_url=base, timeout=30) as client:
                final = None
                deadline = time.time() + 20
                while time.time() < deadline:
                    j = client.get(f"/api/jobs/{job_id}").json()
                    if j["status"] in ("published", "failed", "cancelled",
                                       "superseded"):
                        final = j
                        break
                    time.sleep(0.1)
                assert final is not None, "接管后未到终态"
                assert final["status"] == "published", final
                sizes = sorted(d["size"] for d in final["version"]["derivatives"])
                assert sizes == [256, 512]
                published_version = final["version"]["id"]

                # 派生图内容完好（旧进程解冻后可能误写/误删，必须防住）。
                for d in final["version"]["derivatives"]:
                    rr = client.get(d["url"])
                    assert rr.status_code == 200
                    assert rr.content[8:12] == b"WEBP"
        finally:
            w1.send_signal(signal.SIGCONT)

        # 旧进程复活并继续跑完它的迟到发布：围栏令牌应使其放弃。
        time.sleep(3.0)
        with httpx.Client(base_url=base, timeout=30) as client:
            j = client.get(f"/api/jobs/{job_id}").json()
            assert j["status"] == "published", (
                f"旧持有者迟到写入翻盘了终态: {j['status']} {j.get('error')}"
            )
            assert j["version"]["id"] == published_version
            state = client.get(f"/api/images/{image['id']}").json()
            assert state["current_version"]["id"] == published_version
            for d in j["version"]["derivatives"]:
                rr = client.get(d["url"])
                assert rr.status_code == 200
                assert rr.content[8:12] == b"WEBP"

        leftovers = list((data_dir / "tmp").glob("*.staging-*"))
        assert not leftovers, f"残留临时文件: {leftovers}"

    finally:
        for p in popens:
            if p.poll() is None:
                # SIGSTOP 过的进程收不到 SIGTERM，先 CONT 再终止。
                try:
                    p.send_signal(signal.SIGCONT)
                except ProcessLookupError:
                    pass
                p.terminate()
                try:
                    p.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    p.kill()
