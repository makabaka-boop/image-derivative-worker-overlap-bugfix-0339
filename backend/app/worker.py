from __future__ import annotations

import os
import signal
import sys
import threading
import uuid

from . import config, service

# 工作线程收到停止信号后中断渲染睡眠；退出前把自己仍持有的租约交回队列。
# 崩溃（SIGKILL）时没有这个流程，遗留作业由租约超时后的认领自动接管。
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
          f"租约TTL={config.WORKER_LEASE_TTL_SECONDS:g}s，"
          f"恢复遗留作业 {reset} 个", flush=True)

    _install_signals()
    semaphore = threading.Semaphore(config.WORKER_CONCURRENCY)
    lock = threading.Lock()
    threads: list[dict] = []   # 仍在运行的处理线程（用于并发槽位统计）
    owned: list[dict] = []     # 已认领、尚未确认到终态的作业（停机释放兜底）
    # 启动时已立即回收过一次，巡检只需在第一个心跳周期后开始。
    reclaim_timer = config.WORKER_HEARTBEAT_INTERVAL_SECONDS

    while not _stop.is_set():
        # 回收已结束线程，腾出并发槽位。名下作业表不在这里裁剪：线程结束
        # 不代表 DB 里作业已离开 processing（abandoned 仍等停机释放/接管）。
        with lock:
            threads[:] = [s for s in threads if s["thread"].is_alive()]
            busy = len(threads)
        if busy >= config.WORKER_CONCURRENCY:
            _stop.wait(config.WORKER_POLL_INTERVAL)
            continue

        # 滚动更新/网络分区后可能出现没有新 worker 启动、但租约已过期的
        # 作业；空闲槽位上顺手回收（认领逻辑本身也会接管，这里仅保证无新
        # pending 时仍能推进）。
        if busy == 0:
            reclaim_timer -= config.WORKER_POLL_INTERVAL
            if reclaim_timer <= 0:
                service._requeue_expired(reason="租约巡检")
                reclaim_timer = max(
                    config.WORKER_POLL_INTERVAL,
                    config.WORKER_HEARTBEAT_INTERVAL_SECONDS,
                )

        job = service.claim_job(worker_id)
        if job is None:
            _stop.wait(config.WORKER_POLL_INTERVAL)
            continue

        def runner(j: dict) -> None:
            terminal = False
            try:
                status = service.process_job(j, worker_id=worker_id, stop=_stop)
                print(f"[{worker_id}] 作业 {j['id']} 终态: {status}", flush=True)
                # abandoned 仍在本 worker 名下（等停机释放/租约超时接管）；
                # 其余状态都是 DB 已确认终态，可从名下表移除。
                terminal = status in ("published", "failed", "superseded")
            except Exception as exc:  # noqa: BLE001
                print(f"[{worker_id}] 作业 {j['id']} 处理异常: {exc}", flush=True)
                try:
                    service._mark_failed(
                        j["id"], worker_id, int(j["fence"]), repr(exc)
                    )
                    terminal = True
                except Exception:  # noqa: BLE001
                    pass
            finally:
                if terminal:
                    with lock:
                        owned[:] = [
                            s for s in owned if s["job_id"] != j["id"]
                        ]
                semaphore.release()

        semaphore.acquire()
        t = threading.Thread(target=runner, args=(job,), daemon=True)
        with lock:
            entry = {"thread": t, "job_id": job["id"], "fence": job["fence"]}
            threads.append(entry)
            owned.append(entry)
        t.start()

    print(f"[{worker_id}] 收到停止信号，等待在途作业退出…", flush=True)
    grace = config.WORKER_SHUTDOWN_GRACE_SECONDS
    wait_event = threading.Event()
    # 在途线程通常在下一个 50ms 检查点即放弃渲染；等它们自然结束，
    # 最多等到宽限上限（防卡死）。
    while grace > 0:
        with lock:
            alive = [s for s in threads if s["thread"].is_alive()]
        if not alive:
            break
        wait_event.wait(timeout=0.1)
        grace -= 0.1

    # 名下仍是 processing 的作业统一交回队列（围栏令牌 +1 作废旧持有者），
    # 新 worker 无需等租约超时即可继续；已到终态/被接管的释放 rowcount=0。
    with lock:
        tracked = list(owned)
    released = 0
    for s in tracked:
        try:
            if service.release_job(s["job_id"], worker_id, s["fence"]):
                released += 1
                print(f"[{worker_id}] 作业 {s['job_id']} 已交回队列",
                      flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[{worker_id}] 释放作业 {s['job_id']} 失败: {exc}",
                  flush=True)
    print(f"[{worker_id}] 退出，交回作业 {released} 个", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(worker_loop())
