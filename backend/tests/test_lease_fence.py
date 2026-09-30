"""租约与围栏令牌：滚动更新时作业归属的核心不变量（service/db 层白盒）。

黑盒的 test_restart.py 用真子进程/信号覆盖端到端；这里直接驱动
claim/heartbeat/release/publish，确定性地验证：

- 活租约：别的 worker 不能接管；心跳可持续续租；
- 过期租约：才允许接管，每次易主 fence+1；
- 旧 fence 的心跳 / 置失败 / 发布 / 释放全部 rowcount=0 或抛 LostLease，
  绝不能翻盘新持有者的终态，也不会多出版本/缺尺寸半成品；
- 启动恢复只回收“无租约/租约过期”的作业，活跃作业原样保留。
"""
from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

import pytest

APP_DIR = Path(os.environ.get("APP_DIR", "/app"))
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from conftest import make_png  # noqa: E402

from app import config, db, service  # noqa: E402


@pytest.fixture
def svc(tmp_path, monkeypatch):
    if not APP_DIR.exists():
        pytest.skip("白盒测试需要容器内 /app 目录")
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_DIR", data)
    monkeypatch.setattr(config, "ORIGINALS_DIR", data / "originals")
    monkeypatch.setattr(config, "DERIVS_DIR", data / "derivatives")
    monkeypatch.setattr(config, "TMP_DIR", data / "tmp")
    monkeypatch.setattr(config, "DB_PATH", data / "app.db")
    monkeypatch.setattr(config, "WORKER_LEASE_TTL_SECONDS", 30.0)
    service.startup()
    return service


def _new_job(svc, box=(0, 0, 300, 200), delay_ms=0):
    res = svc.save_upload(make_png(600, 400), "image/png")
    image_id = res["image"]["id"]
    out = svc.submit_job(image_id, box, test_delay_ms=delay_ms)
    return out["job"]["id"]


def _row(conn, job_id):
    return conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()


def test_live_lease_is_not_claimable_and_heartbeat_renews(svc):
    jid = _new_job(svc)

    job = svc.claim_job("w1")
    assert job is not None and job["id"] == jid
    assert job["status"] == "processing" and job["worker_id"] == "w1"
    assert job["fence"] == 1
    first_until = job["lease_until"]
    assert first_until > time.time()

    # 活租约：别的 worker 既看不到 pending，也不允许接管。
    assert svc.claim_job("w2") is None
    with db.tx(immediate=False) as conn:
        row = _row(conn, jid)
        assert row["worker_id"] == "w1" and row["fence"] == 1

    # 心跳续租成功，且旧 fence 续租失败。
    assert svc.heartbeat_job(jid, "w1", 1) is True
    with db.tx(immediate=False) as conn:
        assert _row(conn, jid)["lease_until"] >= first_until
    assert svc.heartbeat_job(jid, "w2", 1) is False  # worker 不匹配
    assert svc.heartbeat_job(jid, "w1", 999) is False  # fence 不匹配


def test_expired_lease_is_reclaimed_with_fence_bump(svc, monkeypatch):
    jid = _new_job(svc)
    svc.claim_job("w1")

    # 租约未过期不能接管。
    assert svc.claim_job("w2") is None

    # 模拟 w1 崩溃后 TTL 流逝。
    with db.tx() as conn:
        conn.execute(
            "UPDATE jobs SET lease_until=? WHERE id=?", (time.time() - 1, jid)
        )

    job = svc.claim_job("w2")
    assert job is not None and job["id"] == jid
    assert job["worker_id"] == "w2" and job["fence"] == 2

    # w1 的旧令牌全部作废。
    assert svc.heartbeat_job(jid, "w1", 1) is False
    assert svc.release_job(jid, "w1", 1) is False
    assert svc.claim_job("w1") is None  # w2 续租有效


def test_stale_owner_cannot_mark_failed_or_publish(svc):
    jid = _new_job(svc)
    svc.claim_job("w1")
    with db.tx() as conn:
        conn.execute(
            "UPDATE jobs SET lease_until=? WHERE id=?", (time.time() - 1, jid)
        )
    job2 = svc.claim_job("w2")
    assert job2["fence"] == 2

    # 旧持有者迟到的失败回写：必须 0 行命中，作业仍是 w2 名下 processing。
    svc._mark_failed(jid, "w1", 1, "迟到的失败")
    with db.tx(immediate=False) as conn:
        row = _row(conn, jid)
        assert row["status"] == "processing"
        assert row["worker_id"] == "w2" and row["fence"] == 2
        assert row["error"] is None

    # 旧持有者迟到发布（携带它自己渲染的 staging）：抛 LostLease，
    # 不产生任何版本/派生图登记。
    staged = [
        {"size": 256, "key": "a", "tmp": config.TMP_DIR / "x1", "w": 256, "h": 171},
        {"size": 512, "key": "b", "tmp": config.TMP_DIR / "x2", "w": 512, "h": 341},
    ]
    with pytest.raises(svc.LostLease):
        svc.publish_rendered(jid, "w1", 1, staged)
    with db.tx(immediate=False) as conn:
        assert conn.execute("SELECT COUNT(*) FROM versions").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM derivatives").fetchone()[0] == 0

    # 真正的持有者 w2 正常完成：published、两尺寸齐全。
    status = svc.process_job(job2, worker_id="w2", stop=threading.Event())
    assert status == "published"
    with db.tx(immediate=False) as conn:
        row = _row(conn, jid)
        assert row["status"] == "published" and row["version_id"]
        assert conn.execute("SELECT COUNT(*) FROM versions").fetchone()[0] == 1
        n = conn.execute(
            "SELECT COUNT(*) FROM version_derivatives WHERE version_id=?",
            (row["version_id"],),
        ).fetchone()[0]
        assert n == 2

    # w1 在 w2 发布之后再失败/再发布，依旧无法翻盘终态。
    svc._mark_failed(jid, "w1", 1, "更晚的失败")
    with pytest.raises(svc.LostLease):
        svc.publish_rendered(jid, "w1", 1, staged)
    with db.tx(immediate=False) as conn:
        row = _row(conn, jid)
        assert row["status"] == "published"
        assert conn.execute("SELECT COUNT(*) FROM versions").fetchone()[0] == 1


def test_graceful_release_hands_back_and_invalidates_fence(svc):
    jid = _new_job(svc)
    svc.claim_job("w1")
    assert svc.release_job(jid, "w1", 1) is True

    with db.tx(immediate=False) as conn:
        row = _row(conn, jid)
        assert row["status"] == "pending"
        assert row["worker_id"] is None and row["lease_until"] is None
        assert row["fence"] == 2

    # 重复释放 / 旧令牌释放都是 0 行。
    assert svc.release_job(jid, "w1", 1) is False

    job = svc.claim_job("w2")
    assert job["id"] == jid and job["fence"] == 3
    status = svc.process_job(job, worker_id="w2", stop=threading.Event())
    assert status == "published"


def test_startup_recovery_skips_live_leases(svc):
    live = _new_job(svc)
    stale = _new_job(svc, box=(10, 10, 100, 100))
    legacy = _new_job(svc, box=(20, 20, 100, 100))

    # 三个 pending 全部被 w1 认领（按创建顺序），fence 均为 1。
    claimed = []
    while True:
        j = svc.claim_job("w1")
        if j is None:
            break
        claimed.append(j["id"])
    assert set(claimed) == {live, stale, legacy}

    with db.tx() as conn:
        # 一个置为过期（崩溃），一个清掉租约信息（老版本遗留），
        # live 保持有效租约。
        conn.execute("UPDATE jobs SET lease_until=? WHERE id=?",
                     (time.time() - 5, stale))
        conn.execute("UPDATE jobs SET lease_until=NULL WHERE id=?", (legacy,))

    assert svc.recover_crashed_jobs() == 2

    with db.tx(immediate=False) as conn:
        live_row = _row(conn, live)
        stale_row = _row(conn, stale)
        legacy_row = _row(conn, legacy)
        assert live_row["status"] == "processing" and live_row["worker_id"] == "w1"
        assert stale_row["status"] == "pending" and stale_row["fence"] == 2
        assert legacy_row["status"] == "pending" and legacy_row["fence"] == 2
        # 活作业 fence 未变。
        assert live_row["fence"] == 1
