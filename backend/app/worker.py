from __future__ import annotations

import os
import signal
import sys
import threading
import uuid

from . import config, service

# 工作线程收到停止信号后中断渲染睡眠，遗留的 processing 作业由下次启动恢复。
_stop = threading.Event()


def _install_signals() -> None:
    def handler(signum, frame):  # noqa: ARG001
        _stop.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def worker_loop() -> int:
    service.startup()
    reset = service.recover_crashed_jobs()
    worker_id = f"w-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    print(f"[{worker_id}] 启动，并发={config.WORKER_CONCURRENCY}，"
          f"恢复遗留作业 {reset} 个", flush=True)

    _install_signals()
    semaphore = threading.Semaphore(config.WORKER_CONCURRENCY)
    in_flight: list[threading.Thread] = []

    while not _stop.is_set():
        # 回收已结束线程，腾出并发槽位。
        in_flight = [t for t in in_flight if t.is_alive()]
        if len(in_flight) >= config.WORKER_CONCURRENCY:
            _stop.wait(config.WORKER_POLL_INTERVAL)
            continue
        job = service.claim_job(worker_id)
        if job is None:
            _stop.wait(config.WORKER_POLL_INTERVAL)
            continue

        def runner(j: dict) -> None:
            try:
                status = service.process_job(j, stop=_stop)
                print(f"[{worker_id}] 作业 {j['id']} 终态: {status}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[{worker_id}] 作业 {j['id']} 处理异常: {exc}", flush=True)
                try:
                    service._mark_failed(j["id"], repr(exc))
                except Exception:  # noqa: BLE001
                    pass
            finally:
                semaphore.release()

        semaphore.acquire()
        t = threading.Thread(target=runner, args=(job,), daemon=True)
        t.start()
        in_flight.append(t)

    print(f"[{worker_id}] 收到停止信号，等待在途作业退出…", flush=True)
    deadline = threading.Event()
    deadline.wait(timeout=8)
    return 0


if __name__ == "__main__":
    sys.exit(worker_loop())
