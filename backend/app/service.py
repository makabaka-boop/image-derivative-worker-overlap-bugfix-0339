"""API 与 worker 共用的业务逻辑层。

作业状态迁移的关键不变量：

- **租约（lease）**：worker 认领作业后获得带过期时间的租约，心跳线程
  按 TTL/3 续租。只有租约过期的 processing 作业才允许被接管；滚动更新时
  新旧进程并存，仍在续租的活作业绝不能被重新入队重复处理。
- **围栏令牌（fence）**：每次认领/接管/释放都让 fence+1。心跳、置失败、
  发布全部带 `worker_id + fence` 守卫，旧持有者即使迟到完成，所有写操作
  都不会命中——不可能把新进程已发布的作业打回 failed 或写出半成品。
- **取消/发布串行化**：均由 BEGIN IMMEDIATE 与条件更新保证只有一个终态。
"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import config, db, imaging, storage


class ServiceError(Exception):
    def __init__(self, message: str, status: int = 400, code: str = "bad_request"):
        super().__init__(message)
        self.status = status
        self.code = code


class LostLease(Exception):
    """作业已不再归本 worker 所有（被取消、被接管或被停机释放）。

    调用方必须放弃本次处理：不动作业终态、不删别人的文件，
    留给当前持有者或下次重试。
    """


def _uuid() -> str:
    return uuid.uuid4().hex


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def spec_key(sha256_hex: str, box: tuple[int, int, int, int], target: int) -> str:
    x, y, w, h = box
    raw = f"{sha256_hex}|{x},{y},{w},{h}|{target}".encode()
    return hashlib.sha256(raw).hexdigest()


# ---------------------------------------------------------------- 启动与恢复

def startup() -> None:
    storage.init_storage()
    db.init_db()


def recover_crashed_jobs() -> int:
    """worker 启动时调用：把无有效持有者的遗留作业退回 pending。

    滚动更新期间旧 worker 可能仍在正常续租，这些作业**不能动**；
    只回收两类：
      1. 老版本遗留、没有租约信息（lease_until IS NULL）的 processing 作业；
      2. 租约已经过期、持有者确实死掉/卡住的作业。
    临时文件不清空整个 tmp/：接管具体作业时只清该作业的旧 staging，
    活 worker 正在写的文件与 API 正在上传的原图临时文件因此不会被误删。
    """
    return _requeue_expired(reason="启动恢复")


def _requeue_expired(reason: str) -> int:
    now_ts = time.time()
    with db.tx() as conn:
        ids = [
            r["id"]
            for r in conn.execute(
                """SELECT id FROM jobs
                    WHERE status='processing'
                      AND (lease_until IS NULL OR lease_until < ?)""",
                (now_ts,),
            ).fetchall()
        ]
        if not ids:
            return 0
        conn.execute(
            """
            UPDATE jobs
               SET status='pending', worker_id=NULL, lease_until=NULL,
                   fence=fence+1, updated_at=?
             WHERE status='processing'
               AND (lease_until IS NULL OR lease_until < ?)
            """,
            (now(), now_ts),
        )
    # 事务外按作业清理旧持有者的 staging：能走到这里说明租约已过期、
    # 没有活持有者；下一个认领者会用全新文件名重写。
    for job_id in ids:
        storage.discard_job_staging(job_id)
    print(f"[recovery] {reason}：回收过期/遗留作业 {len(ids)} 个", flush=True)
    return len(ids)


def heartbeat_job(job_id: str, worker_id: str, fence: int) -> bool:
    """续租。仅当作业仍归本 worker（worker_id+fence 匹配且在 processing）
    时成功；返回 False 表示归属已丢失，worker 应立即放弃。"""
    until = time.time() + config.WORKER_LEASE_TTL_SECONDS
    with db.tx() as conn:
        cur = conn.execute(
            """UPDATE jobs SET lease_until=?
                WHERE id=? AND status='processing'
                  AND worker_id=? AND fence=?""",
            (until, job_id, worker_id, fence),
        )
        return cur.rowcount == 1


def release_job(job_id: str, worker_id: str, fence: int) -> bool:
    """优雅停机：把自己仍持有的作业交回队列，围栏令牌作废。

    旧进程在滚动更新结束前释放租约，新进程可立刻认领而不必等租约超时。
    已被取消/接管/发布的作业 rowcount=0，不触碰。
    """
    with db.tx() as conn:
        cur = conn.execute(
            """UPDATE jobs
                  SET status='pending', worker_id=NULL, lease_until=NULL,
                      fence=fence+1, updated_at=?
                WHERE id=? AND status='processing'
                  AND worker_id=? AND fence=?""",
            (now(), job_id, worker_id, fence),
        )
        return cur.rowcount == 1


def reset_all_for_tests() -> None:
    """仅在 TEST_HOOKS 开启时可用：清空作业数据与产物，供验收测试隔离用。"""
    with db.tx() as conn:
        conn.execute(
            "UPDATE images SET current_version_id=NULL, current_job_id=NULL"
        )
        conn.execute("DELETE FROM version_derivatives")
        conn.execute("DELETE FROM derivatives")
        conn.execute("DELETE FROM jobs")
        conn.execute("DELETE FROM versions")
        conn.execute("DELETE FROM images")
    for directory in (config.ORIGINALS_DIR, config.DERIVS_DIR, config.TMP_DIR):
        if not directory.exists():
            continue
        for p in directory.iterdir():
            if p.is_file() or p.is_symlink():
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass


# ---------------------------------------------------------------- 上传

def save_upload(data: bytes, declared_content_type: str) -> dict[str, Any]:
    if len(data) == 0:
        raise ServiceError("空文件", status=400, code="empty_file")
    info = imaging.validate(data, declared_content_type)
    digest = hashlib.sha256(data).hexdigest()
    path = storage.original_path(digest, info.fmt)
    storage.save_original_atomic(path, data)

    image_id = _uuid()
    ts = now()
    try:
        with db.tx() as conn:
            existing = conn.execute(
                "SELECT * FROM images WHERE sha256=?", (digest,)
            ).fetchone()
            if existing:
                return {"reused": True, "image": _serialize_image(conn, existing)}
            conn.execute(
                """INSERT INTO images
                   (id, sha256, format, width, height, size_bytes,
                    original_path, current_version_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (image_id, digest, info.fmt, info.width, info.height, len(data),
                 str(path), None, ts),
            )
            row = conn.execute("SELECT * FROM images WHERE id=?", (image_id,)).fetchone()
            return {"reused": False, "image": _serialize_image(conn, row)}
    except sqlite3.IntegrityError:
        # 并发上传同一内容：另一事务已插入，直接复用。
        with db.tx() as conn:
            row = conn.execute(
                "SELECT * FROM images WHERE sha256=?", (digest,)
            ).fetchone()
            return {"reused": True, "image": _serialize_image(conn, row)}


# ---------------------------------------------------------------- 申请作业

def submit_job(
    image_id: str,
    box: tuple[int, int, int, int],
    test_delay_ms: int = 0,
) -> dict[str, Any]:
    x, y, w, h = box
    with db.tx() as conn:
        image = conn.execute(
            "SELECT * FROM images WHERE id=?", (image_id,)
        ).fetchone()
        if image is None:
            raise ServiceError("图片不存在", status=404, code="not_found")
        info = imaging.ImageInfo(image["format"], image["width"], image["height"])
        imaging.validate_crop(info, x, y, w, h)

        ts = now()

        # 1) 相同原图摘要 + 相同裁切参数：复用已完成版本，立即返回 published。
        version = conn.execute(
            """SELECT * FROM versions
                WHERE image_id=? AND crop_x=? AND crop_y=? AND crop_w=? AND crop_h=?""",
            (image_id, x, y, w, h),
        ).fetchone()
        if version is not None:
            job_id = _uuid()
            conn.execute(
                """INSERT INTO jobs
                   (id, image_id, crop_x, crop_y, crop_w, crop_h, status,
                    version_id, test_delay_ms, worker_id, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,'published', ?, ?, NULL, ?, ?)""",
                (job_id, image_id, x, y, w, h, version["id"], 0, ts, ts),
            )
            conn.execute(
                "UPDATE images SET current_version_id=?, current_job_id=? WHERE id=?",
                (version["id"], job_id, image_id),
            )
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return {"http_status": 200, "reused": True,
                    "job": _serialize_job(conn, row)}

        # 2) 同参数已有在途作业：直接复用，不新建。
        inflight = conn.execute(
            """SELECT * FROM jobs
                WHERE image_id=? AND crop_x=? AND crop_y=? AND crop_w=? AND crop_h=?
                  AND status IN ('pending','processing')
                ORDER BY created_at DESC LIMIT 1""",
            (image_id, x, y, w, h),
        ).fetchone()
        if inflight is not None:
            conn.execute(
                "UPDATE images SET current_job_id=? WHERE id=?",
                (inflight["id"], image_id),
            )
            return {"http_status": 202, "reused": True,
                    "job": _serialize_job(conn, inflight)}

        # 3) 新参数：新建 pending 作业，成为当前选择。
        # 旧的已发布版本继续保留展示，只移动当前作业指针。
        job_id = _uuid()
        conn.execute(
            """INSERT INTO jobs
               (id, image_id, crop_x, crop_y, crop_w, crop_h, status,
                version_id, test_delay_ms, created_at, updated_at)
               VALUES (?,?,?,?,?,?,'pending', NULL, ?, ?, ?)""",
            (job_id, image_id, x, y, w, h, test_delay_ms, ts, ts),
        )
        conn.execute(
            "UPDATE images SET current_job_id=? WHERE id=?", (job_id, image_id)
        )
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return {"http_status": 202, "reused": False,
                "job": _serialize_job(conn, row)}


# ---------------------------------------------------------------- 查询

def get_image(image_id: str) -> dict[str, Any]:
    with db.tx(immediate=False) as conn:
        row = conn.execute("SELECT * FROM images WHERE id=?", (image_id,)).fetchone()
        if row is None:
            raise ServiceError("图片不存在", status=404, code="not_found")
        return _serialize_image(conn, row)


def list_images() -> list[dict[str, Any]]:
    with db.tx(immediate=False) as conn:
        rows = conn.execute(
            "SELECT * FROM images ORDER BY created_at DESC"
        ).fetchall()
        return [_serialize_image(conn, r) for r in rows]


def get_job(job_id: str) -> dict[str, Any]:
    with db.tx(immediate=False) as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise ServiceError("作业不存在", status=404, code="not_found")
        return _serialize_job(conn, row)


def get_original_file(image_id: str) -> dict[str, str]:
    with db.tx(immediate=False) as conn:
        row = conn.execute(
            "SELECT * FROM images WHERE id=?", (image_id,)
        ).fetchone()
        if row is None:
            raise ServiceError("图片不存在", status=404, code="not_found")
        if not Path(row["original_path"]).exists():
            raise ServiceError("原图文件缺失", status=410, code="gone")
        return {"path": row["original_path"], "format": row["format"]}


def get_derivative(deriv_id: str) -> dict[str, str]:
    with db.tx(immediate=False) as conn:
        row = conn.execute(
            "SELECT * FROM derivatives WHERE id=?", (deriv_id,)
        ).fetchone()
        if row is None:
            raise ServiceError("派生图不存在", status=404, code="not_found")
        if not Path(row["path"]).exists():
            raise ServiceError("派生图文件缺失", status=410, code="gone")
        return {"path": row["path"]}


# ---------------------------------------------------------------- 取消

def cancel_job(job_id: str) -> dict[str, Any]:
    with db.tx() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise ServiceError("作业不存在", status=404, code="not_found")
        if row["status"] == "cancelled":
            return {"job": _serialize_job(conn, row), "changed": False}
        if row["status"] in ("published", "superseded", "failed"):
            raise ServiceError(
                "作业已进入终态，无法取消", status=409, code="terminal"
            )
        # pending / processing 都由条件更新兜底；若发布事务先提交，
        # rowcount 为 0，下面按终态冲突处理。同时清空 worker_id/租约并
        # 推进 fence，持有者下一次心跳/发布即因归属不匹配而放弃。
        cur = conn.execute(
            """UPDATE jobs
                  SET status='cancelled', worker_id=NULL, lease_until=NULL,
                      fence=fence+1, updated_at=?
                WHERE id=? AND status IN ('pending','processing')""",
            (now(), job_id),
        )
        if cur.rowcount == 0:
            fresh = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            raise ServiceError(
                "作业已进入终态，无法取消", status=409, code="terminal"
            )
        # 仅当取消的是当前选择时清空指针。
        conn.execute(
            """UPDATE images SET current_job_id=NULL
                WHERE id=? AND current_job_id=?""",
            (row["image_id"], job_id),
        )
        fresh = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return {"job": _serialize_job(conn, fresh), "changed": True}


# ---------------------------------------------------------------- worker 执行

def claim_job(worker_id: str) -> dict[str, Any] | None:
    """认领一个 pending 作业，或接管租约过期的 processing 作业。

    两类候选在同一事务里按时间排序，状态迁移全部带条件更新（防并发双领），
    fence 每易主一次 +1。接管时在事务外清掉旧持有者的 staging 文件——
    那只会是过期 worker 的遗留物（活持有者的文件名带不同 fence 且作业
    不会被接管）。

    兜底：submit 的“查版本 → 查在途 → 插入”之间存在窗口，极端情况下
    同参数可能漏建出第二个在途作业。认领时若同参数版本已由更早的作业
    发布，就把这个冗余作业直接收敛到该版本（published），不再渲染，
    从根上杜绝重复版本。
    """
    now_ts = time.time()
    with db.tx() as conn:
        row = conn.execute(
            """SELECT * FROM jobs
                WHERE status='pending'
                   OR (status='processing' AND lease_until < ?)
                ORDER BY created_at ASC, id ASC LIMIT 1""",
            (now_ts,),
        ).fetchone()
        if row is None:
            return None

        version = conn.execute(
            """SELECT id FROM versions
                WHERE image_id=? AND crop_x=? AND crop_y=?
                  AND crop_w=? AND crop_h=?""",
            (row["image_id"], row["crop_x"], row["crop_y"],
             row["crop_w"], row["crop_h"]),
        ).fetchone()
        if version is not None:
            # 同参数版本已被另一个在途作业抢先发布：冗余作业直接复用它。
            done = conn.execute(
                """UPDATE jobs
                      SET status='published', version_id=?, worker_id=NULL,
                          lease_until=NULL, updated_at=?
                    WHERE id=? AND status IN ('pending','processing')
                      AND (status='pending' OR lease_until < ?)""",
                (version["id"], now(), row["id"], now_ts),
            )
            if done.rowcount == 1:
                return None  # 本拍无可执行作业，循环继续认领下一个。

        reclaimed = row["status"] == "processing"
        lease_until = now_ts + config.WORKER_LEASE_TTL_SECONDS
        cur = conn.execute(
            """UPDATE jobs
                  SET status='processing', worker_id=?, lease_until=?,
                      fence=fence+1, updated_at=?
                WHERE id=?
                  AND (status='pending'
                       OR (status='processing' AND lease_until < ?))""",
            (worker_id, lease_until, now(), row["id"], now_ts),
        )
        if cur.rowcount == 0:
            return None
        fresh = conn.execute(
            "SELECT * FROM jobs WHERE id=?", (row["id"],)
        ).fetchone()
        job = dict(fresh)
        job["_reclaimed"] = reclaimed
    if reclaimed:
        storage.discard_job_staging(job["id"])
    return job


def process_job(
    job: dict[str, Any],
    worker_id: str,
    stop: threading.Event | None = None,
) -> str:
    """渲染两个尺寸并一次性发布。返回最终状态。

    期间由独立心跳线程续租；若续租失败（作业被取消/被新 worker 接管）
    立即抛 LostLease 放弃——不写终态、不覆盖别人的文件。
    """
    job_id = job["id"]
    fence = int(job["fence"])
    lease_lost = threading.Event()
    heartbeat_stop = threading.Event()

    def _beat() -> None:
        interval = config.WORKER_HEARTBEAT_INTERVAL_SECONDS
        while not heartbeat_stop.wait(interval):
            try:
                if not heartbeat_job(job_id, worker_id, fence):
                    lease_lost.set()
                    return
            except sqlite3.Error:
                # 偶发锁竞争/数据库抖动：下一拍再试，连续失败由租约超时兜底。
                pass

    beater = threading.Thread(target=_beat, name=f"hb-{job_id[:8]}", daemon=True)
    beater.start()

    # 在外层声明：无论是停机还是丢租约中断，本进程自己写的 staging 都要
    # 在 finally 里清掉（接管者使用不同围栏的文件名，绝不会误删别人的）。
    staged: list[dict[str, Any]] = []
    try:
        conn = db.connect()
        try:
            image = conn.execute(
                "SELECT * FROM images WHERE id=?", (job["image_id"],)
            ).fetchone()
        finally:
            conn.close()
        data = Path(image["original_path"]).read_bytes()
        box = (job["crop_x"], job["crop_y"], job["crop_w"], job["crop_h"])

        delay = max(0, int(job["test_delay_ms"])) / 1000.0
        # 拆成三段：渲染前 / 渲染间隙 / 渲染后，使取消窗口稳定可测。
        slice_ = delay / (len(config.SIZES) + 1)

        try:
            for target in config.SIZES:
                _interruptible(slice_, stop, lease_lost)
                webp, out_w, out_h = imaging.render_webp(
                    data, image["format"], box, target
                )
                key = spec_key(image["sha256"], box, target)
                # 每次渲染生成全新唯一文件名：同一作业被接管重跑、两个进程
                # 并存时绝不共用临时路径，不可能互相截断或删除。
                tmp = storage.staging_path(key, job_id, target, fence)
                storage.write_staged_deriv(tmp, webp)
                staged.append({"size": target, "key": key, "tmp": tmp,
                               "w": out_w, "h": out_h})
            _interruptible(slice_, stop, lease_lost)
        except Exception as exc:
            if isinstance(exc, (_Shutdown, LostLease)):
                raise
            # 坏图/编码失败：仅持有者可置 failed。
            _mark_failed(job_id, worker_id, fence, str(exc))
            return "failed"

        # 发布成功时文件已被 os.replace 移走；取消/丢租约/停机时由 finally
        # 统一清理仍在磁盘上的 staging。
        return publish_rendered(job_id, worker_id, fence, staged)
    except _Shutdown:
        return _abandoned(job_id)
    except LostLease:
        return "abandoned"
    finally:
        _discard_staging(staged)
        heartbeat_stop.set()
        beater.join(timeout=1)


class _Shutdown(Exception):
    """收到停机信号，等待外层统一释放租约。"""


def _interruptible(
    seconds: float,
    stop: threading.Event | None,
    lease_lost: threading.Event,
) -> None:
    """睡眠等待，同时响应停机与丢租约；每 50ms 检查一次归属。"""
    if seconds <= 0:
        if stop is not None and stop.is_set():
            raise _Shutdown
        if lease_lost.is_set():
            raise LostLease
        return
    deadline = time.monotonic() + seconds
    while True:
        if stop is not None and stop.is_set():
            raise _Shutdown
        if lease_lost.is_set():
            raise LostLease
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        stop.wait(min(0.05, remaining)) if stop is not None else time.sleep(
            min(0.05, remaining)
        )


def _discard_staging(staged: list[dict[str, Any]]) -> None:
    for item in staged:
        try:
            item["tmp"].unlink(missing_ok=True)
        except OSError:
            pass


def _abandoned(job_id: str) -> str:
    """停机信号打断：不改终态，停机流程把租约交回 pending（崩溃则等超时接管）。"""
    return "abandoned"


def _mark_failed(
    job_id: str, worker_id: str, fence: int, error: str
) -> None:
    with db.tx() as conn:
        conn.execute(
            """UPDATE jobs SET status='failed', error=?, worker_id=NULL,
                              lease_until=NULL, updated_at=?
                WHERE id=? AND status='processing'
                  AND worker_id=? AND fence=?""",
            (error[:500], now(), job_id, worker_id, fence),
        )


def publish_rendered(
    job_id: str,
    worker_id: str,
    fence: int,
    staged: list[dict[str, Any]],
) -> str:
    """两个尺寸都渲染好之后，在同一个事务内发布。

    全程带 worker_id+fence 围栏守卫：旧持有者迟到发布时 rowcount 全部为 0，
    直接放弃。带锁冲突重试。
    """
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with db.tx() as conn:
                row = conn.execute(
                    "SELECT * FROM jobs WHERE id=?", (job_id,)
                ).fetchone()
                if (
                    row["status"] != "processing"
                    or row["worker_id"] != worker_id
                    or row["fence"] != fence
                ):
                    # 取消先赢、已被接管或易主：丢弃成果，终态由别人决定。
                    raise LostLease(job_id)

                # 文件先落正式位置（内容寻址；崩溃留下孤儿文件也无害、可复用）。
                for item in staged:
                    dst = storage.deriv_path(item["key"])
                    if item["tmp"].exists():
                        storage.publish_deriv(item["tmp"], dst)
                    if not dst.exists():
                        # 自己的临时文件不可能被别人删（名字唯一）；缺失说明
                        # 磁盘异常，直接失败而不是写出缺尺寸的版本。
                        raise RuntimeError("派生图文件丢失")
                    item["path"] = str(dst)

                ts = now()
                version_id = _uuid()
                conn.execute(
                    """INSERT OR IGNORE INTO versions
                       (id, image_id, crop_x, crop_y, crop_w, crop_h,
                        version_no, created_at)
                       VALUES (?,?,?,?,?,?,
                          (SELECT COALESCE(MAX(version_no),0)+1 FROM versions
                            WHERE image_id=?), ?)""",
                    (version_id, row["image_id"], row["crop_x"], row["crop_y"],
                     row["crop_w"], row["crop_h"], row["image_id"], ts),
                )
                version = conn.execute(
                    """SELECT * FROM versions
                        WHERE image_id=? AND crop_x=? AND crop_y=?
                          AND crop_w=? AND crop_h=?""",
                    (row["image_id"], row["crop_x"], row["crop_y"],
                     row["crop_w"], row["crop_h"]),
                ).fetchone()
                version_id = version["id"]

                for item in staged:
                    deriv_id = _uuid()
                    w, h = item["w"], item["h"]
                    conn.execute(
                        """INSERT OR IGNORE INTO derivatives
                           (id, image_id, size_px, key, width, height,
                            path, created_at)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (deriv_id, row["image_id"], item["size"], item["key"],
                         w, h, item["path"], ts),
                    )
                    deriv = conn.execute(
                        "SELECT * FROM derivatives WHERE key=?", (item["key"],)
                    ).fetchone()
                    conn.execute(
                        """INSERT OR IGNORE INTO version_derivatives
                           (version_id, derivative_id, size_px)
                           VALUES (?,?,?)""",
                        (version_id, deriv["id"], item["size"]),
                    )

                # 关键守卫：只有该作业仍是图片的当前选择时，才更新发布指针。
                guard = conn.execute(
                    """UPDATE images
                          SET current_version_id=?
                        WHERE id=? AND current_job_id=?""",
                    (version_id, row["image_id"], job_id),
                )
                new_status = "published" if guard.rowcount == 1 else "superseded"
                done = conn.execute(
                    """UPDATE jobs
                          SET status=?, version_id=?, worker_id=NULL,
                              lease_until=NULL, updated_at=?
                        WHERE id=? AND status='processing'
                          AND worker_id=? AND fence=?""",
                    (new_status, version_id, ts, job_id, worker_id, fence),
                )
                if done.rowcount == 0:
                    # 事务内文件已替换但归属刚好丢失：回滚数据库改动，
                    # 磁盘成果留待复用，调用方按放弃处理。
                    raise LostLease(job_id)
                return new_status
        except LostLease:
            raise
        except sqlite3.OperationalError as exc:
            last_exc = exc
            threading.Event().wait(0.1 * (attempt + 1))
    raise RuntimeError(f"发布作业 {job_id} 失败: {last_exc}")


# ---------------------------------------------------------------- 序列化

def _version_payload(conn: sqlite3.Connection, version_id: str | None) -> dict | None:
    if version_id is None:
        return None
    version = conn.execute(
        "SELECT * FROM versions WHERE id=?", (version_id,)
    ).fetchone()
    if version is None:
        return None
    derivs = []
    for d in conn.execute(
        """SELECT d.* FROM derivatives d
             JOIN version_derivatives vd ON vd.derivative_id=d.id
            WHERE vd.version_id=? ORDER BY d.size_px""",
        (version_id,),
    ).fetchall():
        derivs.append({
            "id": d["id"],
            "size": d["size_px"],
            "width": d["width"],
            "height": d["height"],
            "url": f"/api/derivatives/{d['id']}",
        })
    return {
        "id": version["id"],
        "version_no": version["version_no"],
        "crop": {"x": version["crop_x"], "y": version["crop_y"],
                 "w": version["crop_w"], "h": version["crop_h"]},
        "created_at": version["created_at"],
        "derivatives": derivs,
    }


def _serialize_job(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "image_id": row["image_id"],
        "status": row["status"],
        "crop": {"x": row["crop_x"], "y": row["crop_y"],
                 "w": row["crop_w"], "h": row["crop_h"]},
        "error": row["error"],
        "version": _version_payload(conn, row["version_id"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _serialize_image(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    current_job = None
    if row["current_job_id"]:
        j = conn.execute(
            "SELECT * FROM jobs WHERE id=?", (row["current_job_id"],)
        ).fetchone()
        if j is not None:
            current_job = {
                "id": j["id"],
                "status": j["status"],
                "crop": {"x": j["crop_x"], "y": j["crop_y"],
                         "w": j["crop_w"], "h": j["crop_h"]},
            }
    return {
        "id": row["id"],
        "sha256": row["sha256"],
        "format": row["format"],
        "width": row["width"],
        "height": row["height"],
        "size_bytes": row["size_bytes"],
        "current_version": _version_payload(conn, row["current_version_id"]),
        "current_job": current_job,
        "created_at": row["created_at"],
    }
