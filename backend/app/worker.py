from __future__ import annotations

import os
import signal
import sys
import threading
import time
import uuid

from . import config, service

# 进程级停止信号：收到后不再认领新作业；已认领的作业在宽限期内继续做完，
# 其租约由各自的心跳线程维持，能完成的作业照常发布、不需要重试。
_stop = threading.Event()


def _install_signals() -> None:
    def handler(signum, frame):  # noqa: ARG001
        _stop.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def worker_loop() -> int:
    service.startup()
    worker_id = f"w-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    # 启动时只回收租约已过期的作业——滚动更新时旧进程可能仍在处理其作业。
    reset = service.requeue_expired_jobs()
    print(f"[{worker_id}] 启动，并发={config.WORKER_CONCURRENCY}，"
          f"回收过期租约作业 {reset} 个", flush=True)

    _install_signals()
    semaphore = threading.Semaphore(config.WORKER_CONCURRENCY)
    in_flight: list[threading.Thread] = []
    next_reconcile = 0.0

    while not _stop.is_set():
        # 周期性对账：回收过期租约（SIGKILL/卡死的作业）、清扫终态残留文件。
        mono = time.monotonic()
        if mono >= next_reconcile:
            next_reconcile = mono + config.RECONCILE_INTERVAL_SECONDS
            try:
                service.reconcile(worker_id)
            except Exception as exc:  # noqa: BLE001
                print(f"[{worker_id}] 对账异常: {exc}", flush=True)

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
            # 每个作业独立的中断事件：只有丢失租约（被取消/被别人接管）
            # 才中断渲染；进程收到 SIGTERM 不打断作业，让它在宽限期内做完。
            job_stop = threading.Event()
            try:
                status = service.process_job(
                    j, stop=job_stop, worker_id=worker_id
                )
                print(f"[{worker_id}] 作业 {j['id']} 终态: {status}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[{worker_id}] 作业 {j['id']} 处理异常: {exc}", flush=True)
                try:
                    # 带 worker 栅栏：本进程已丢失租约时不会误写重试作业。
                    service._mark_failed(j["id"], repr(exc), worker_id)
                except Exception:  # noqa: BLE001
                    pass
            finally:
                semaphore.release()

        semaphore.acquire()
        t = threading.Thread(target=runner, args=(job,), daemon=True)
        t.start()
        in_flight.append(t)

    print(f"[{worker_id}] 收到停止信号，停止认领，等待在途作业退出…", flush=True)
    deadline = time.monotonic() + config.SHUTDOWN_GRACE_SECONDS
    for t in list(in_flight):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        t.join(timeout=remaining)
    # 仍未完成的作业保持 processing 与未到期租约；进程退出后心跳停止，
    # 租约 TTL 到期后由存活/下一世代的 worker 对账重新入队并重试。
    return 0


if __name__ == "__main__":
    sys.exit(worker_loop())
