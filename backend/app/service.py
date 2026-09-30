"""API 与 worker 共用的业务逻辑层。

所有作业状态迁移都经过条件更新（WHERE status=...），并由 BEGIN IMMEDIATE
串行化，保证：取消与完成竞争只有一个终态；旧作业迟到完成不会覆盖较新选择。
"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import config, db, imaging, storage

TERMINAL_STATUSES = ("published", "cancelled", "superseded", "failed")


class ServiceError(Exception):
    def __init__(self, message: str, status: int = 400, code: str = "bad_request"):
        super().__init__(message)
        self.status = status
        self.code = code


def _uuid() -> str:
    return uuid.uuid4().hex


def now() -> str:
    # 与租约时间戳保持同样的微秒精度：两者都用字符串字典序比较，
    # 一边有小数秒、一边没有会在同一秒内得出错误的先后顺序。
    return datetime.now(timezone.utc).isoformat()


def lease_expiry(ttl_seconds: float | None = None) -> str:
    # 必须保留微秒精度：秒级截断会让租约提前将近 1 秒被判过期，
    # 在滚动更新的交接窗口内可能误判仍存活的 worker 为死亡。
    ttl = config.LEASE_TTL_SECONDS if ttl_seconds is None else ttl_seconds
    return (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat()


def spec_key(sha256_hex: str, box: tuple[int, int, int, int], target: int) -> str:
    x, y, w, h = box
    raw = f"{sha256_hex}|{x},{y},{w},{h}|{target}".encode()
    return hashlib.sha256(raw).hexdigest()


def interruptible_sleep(seconds: float, stop: threading.Event | None) -> bool:
    """返回 True 表示被要求停止。"""
    if seconds <= 0:
        return bool(stop and stop.is_set())
    if stop is None:
        threading.Event().wait(seconds)
        return False
    return stop.wait(timeout=seconds)


# ---------------------------------------------------------------- 启动与租约

def startup() -> None:
    storage.init_storage()
    db.init_db()


def requeue_expired_jobs() -> int:
    """只把**租约已过期**的 processing 作业退回 pending。

    滚动更新时新进程启动，旧进程可能仍存活并持有未过期租约——这些作业
    绝不能被抢走重复处理；只有确认旧进程已死/停止续租（SIGKILL、卡死、
    SIGTERM 后停止心跳且 TTL 到期）的作业才重新入队。
    """
    ts = now()
    with db.tx() as conn:
        cur = conn.execute(
            """
            UPDATE jobs
               SET status='pending', worker_id=NULL,
                   lease_expires_at=NULL, updated_at=?
             WHERE status='processing'
               AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
            """,
            (ts, ts),
        )
        return cur.rowcount


def heartbeat(job_id: str, worker_id: str) -> bool:
    """续租。返回 False 说明该作业已不属于本 worker（被取消或租约过期后
    被别人重新认领），调用方必须放弃处理、丢弃本地成果。"""
    with db.tx() as conn:
        cur = conn.execute(
            """UPDATE jobs SET lease_expires_at=?, updated_at=updated_at
                WHERE id=? AND status='processing' AND worker_id=?""",
            (lease_expiry(), job_id, worker_id),
        )
        return cur.rowcount == 1


def reconcile(worker_id: str) -> dict[str, int]:
    """周期性对账：回收过期租约 + 清扫已终态作业留下的临时文件。

    由存活 worker 定期执行（而非只在启动时执行一次），保证 SIGKILL 后
    不依赖重启也能恢复，且清扫按作业状态判定，不碰其他进程正在写的文件。
    """
    reset = requeue_expired_jobs()
    with db.connect() as conn:
        terminal = [
            r[0] for r in conn.execute(
                f"SELECT id FROM jobs WHERE status IN "
                f"({','.join('?' for _ in TERMINAL_STATUSES)})",
                TERMINAL_STATUSES,
            )
        ]
    storage.sweep_tmp(terminal, datetime.now(timezone.utc))
    return {"reset": reset}


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
        # rowcount 为 0，下面按终态冲突处理。
        cur = conn.execute(
            """UPDATE jobs
                  SET status='cancelled', worker_id=NULL,
                      lease_expires_at=NULL, updated_at=?
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
    with db.tx() as conn:
        row = conn.execute(
            """SELECT * FROM jobs
                WHERE status='pending'
                ORDER BY created_at ASC, id ASC LIMIT 1"""
        ).fetchone()
        if row is None:
            return None
        cur = conn.execute(
            """UPDATE jobs
                  SET status='processing', worker_id=?,
                      lease_expires_at=?, updated_at=?
                WHERE id=? AND status='pending'""",
            (worker_id, lease_expiry(), now(), row["id"]),
        )
        if cur.rowcount == 0:
            return None
        claimed = conn.execute(
            "SELECT * FROM jobs WHERE id=?", (row["id"],)
        ).fetchone()
        return dict(claimed)


def process_job(job: dict[str, Any], stop: threading.Event | None = None,
                worker_id: str | None = None) -> str:
    """渲染两个尺寸并一次性发布。返回最终状态。

    持有作业租约期间用心跳续租；一旦心跳失败（被取消或租约过期后被别人
    重新认领）立即放弃，本地 staging 文件全部丢弃，绝不触碰数据库终态与
    正式产物——这是滚动更新时"旧进程不能污染新进程结果"的关键栅栏。
    """
    worker_id = worker_id or job.get("worker_id")
    job_id = job["id"]
    lease_lost = threading.Event()

    def _renew() -> None:
        while not lease_lost.wait(config.LEASE_HEARTBEAT_SECONDS):
            if not heartbeat(job_id, worker_id):
                lease_lost.set()
                if stop is not None:
                    stop.set()
                return

    hb = threading.Thread(target=_renew, daemon=True)
    hb.start()

    with db.connect() as conn:
        image = conn.execute(
            "SELECT * FROM images WHERE id=?", (job["image_id"],)
        ).fetchone()
    data = Path(image["original_path"]).read_bytes()
    box = (job["crop_x"], job["crop_y"], job["crop_w"], job["crop_h"])

    # 本次尝试独占 staging 文件名；清掉该作业此前尝试的残留（旧进程被
    # SIGKILL 或已放弃租约时留下的文件），只保留本次新写的文件。
    attempt = uuid.uuid4().hex[:10]
    storage.discard_job_attempts(job_id)

    delay = max(0, int(job["test_delay_ms"])) / 1000.0
    # 拆成三段：渲染前 / 渲染间隙 / 渲染后，使取消窗口稳定可测。
    slice_ = delay / (len(config.SIZES) + 1)

    staged: list[dict[str, Any]] = []
    try:
        for target in config.SIZES:
            if interruptible_sleep(slice_, stop):
                return _abandon(job_id, worker_id, lease_lost, staged)
            webp, out_w, out_h = imaging.render_webp(
                data, image["format"], box, target
            )
            key = spec_key(image["sha256"], box, target)
            tmp = storage.staging_tmp(job_id, attempt, key, target)
            storage.write_staged_deriv(tmp, webp)
            staged.append({"size": target, "key": key, "tmp": tmp,
                           "w": out_w, "h": out_h})
        if interruptible_sleep(slice_, stop):
            return _abandon(job_id, worker_id, lease_lost, staged)
    except Exception as exc:  # 坏图/编码失败：栅栏更新为 failed
        _mark_failed(job_id, str(exc), worker_id)
        _discard_staging(staged)
        return "failed"
    finally:
        lease_lost.set()  # 停心跳
        hb.join(timeout=1)

    status = publish_rendered(job_id, staged, worker_id)
    if status in ("cancelled", "lease_lost"):
        # 取消先赢 / 租约已被别人接管：成果不发布，清掉本地临时文件。
        _discard_staging(staged)
    return status


def _abandon(job_id: str, worker_id: str | None, lease_lost: threading.Event,
             staged: list[dict[str, Any]]) -> str:
    """停机信号或租约丢失打断：不强改终态。

    - 仍持有租约且只是本进程优雅停机：租约会在 TTL 后过期，由对账重新入队；
    - 租约已丢失（作业已被别的 worker 接管）：只清本地文件，绝不碰数据库。
    """
    _discard_staging(staged)
    if lease_lost.is_set() or not _still_owned(job_id, worker_id):
        return "lease_lost"
    return "abandoned"


def _still_owned(job_id: str, worker_id: str | None) -> bool:
    if worker_id is None:
        return False
    with db.connect() as conn:
        row = conn.execute(
            "SELECT status, worker_id, lease_expires_at FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
    if row is None or row["status"] != "processing" or row["worker_id"] != worker_id:
        return False
    return bool(row["lease_expires_at"] and row["lease_expires_at"] > now())


def _discard_staging(staged: list[dict[str, Any]]) -> None:
    for item in staged:
        try:
            item["tmp"].unlink(missing_ok=True)
        except OSError:
            pass


def _mark_failed(job_id: str, error: str, worker_id: str | None = None) -> None:
    with db.tx() as conn:
        # worker_id 栅栏：只有作业仍归本 worker 时才能写 failed。
        # 否则旧进程的异常不得把新进程刚认领的重试打成 failed（failed 是
        # 终态，被误写会永久无法恢复）。
        conn.execute(
            """UPDATE jobs SET status='failed', error=?, worker_id=NULL,
                   lease_expires_at=NULL, updated_at=?
                WHERE id=? AND status='processing' AND worker_id=?""",
            (error[:500], now(), job_id, worker_id),
        )


def publish_rendered(job_id: str, staged: list[dict[str, Any]],
                     worker_id: str | None = None) -> str:
    """两个尺寸都渲染好之后，在同一个事务内发布。带锁冲突重试。

    所有作业状态迁移都带 worker_id 栅栏：只有当前租约持有者能发布，
    旧进程的迟到完成不能改动新进程认领后的作业。
    """
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            with db.tx() as conn:
                row = conn.execute(
                    "SELECT * FROM jobs WHERE id=?", (job_id,)
                ).fetchone()
                if row["status"] != "processing" or row["worker_id"] != worker_id:
                    # 取消先赢（终态已由取消决定）或租约已被别人接管：
                    # 丢弃成果，绝不用旧进程身份覆盖任何状态。
                    return (row["status"] if row["status"] != "processing"
                            else "lease_lost")

                # 文件先落正式位置（内容寻址；崩溃留下孤儿文件也无害、可复用）。
                for item in staged:
                    dst = storage.deriv_path(item["key"])
                    if item["tmp"].exists():
                        storage.publish_deriv(item["tmp"], dst)
                    if not dst.exists():
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
                conn.execute(
                    """UPDATE jobs
                          SET status=?, version_id=?, worker_id=NULL,
                              lease_expires_at=NULL, updated_at=?
                        WHERE id=? AND status='processing' AND worker_id=?""",
                    (new_status, version_id, ts, job_id, worker_id),
                )
                return new_status
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
