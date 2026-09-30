"""取消与完成竞争、旧作业迟到完成不得覆盖当前选择。"""
from __future__ import annotations

import time

from conftest import make_png, submit, upload, wait_image, wait_job


def test_cancel_wins_when_before_completion(client):
    image = upload(client, make_png(512, 512)).json()["image"]
    r = submit(client, image["id"],
               {"x": 0, "y": 0, "w": 400, "h": 400}, delay_ms=1500)
    job_id = r.json()["job"]["id"]

    # 等作业进入 processing，再取消。
    wait_image(
        client, image["id"],
        lambda b: b["current_job"] is not None
        and b["current_job"]["status"] == "processing",
    )
    cancel = client.post(f"/api/jobs/{job_id}/cancel")
    assert cancel.status_code == 200, cancel.text

    # 终态必须是 cancelled，发布不能翻盘。
    time.sleep(2.2)
    final = client.get(f"/api/jobs/{job_id}").json()
    assert final["status"] == "cancelled", final
    state = client.get(f"/api/images/{image['id']}").json()
    assert state["current_version"] is None


def test_cancel_loses_when_already_published(client):
    image = upload(client, make_png(512, 512)).json()["image"]
    r = submit(client, image["id"],
               {"x": 0, "y": 0, "w": 100, "h": 100}, delay_ms=600)
    job_id = r.json()["job"]["id"]

    done = wait_job(client, job_id)
    assert done["status"] == "published"

    cancel = client.post(f"/api/jobs/{job_id}/cancel")
    assert cancel.status_code == 409
    assert cancel.json()["error"]["code"] == "terminal"

    final = client.get(f"/api/jobs/{job_id}").json()
    assert final["status"] == "published"


def test_cancel_idempotent(client):
    image = upload(client, make_png(256, 256)).json()["image"]
    r = submit(client, image["id"],
               {"x": 0, "y": 0, "w": 100, "h": 100}, delay_ms=1500)
    job_id = r.json()["job"]["id"]
    wait_image(
        client, image["id"],
        lambda b: b["current_job"] is not None
        and b["current_job"]["status"] == "processing",
    )
    c1 = client.post(f"/api/jobs/{job_id}/cancel")
    c2 = client.post(f"/api/jobs/{job_id}/cancel")
    assert c1.status_code == 200
    assert c2.status_code == 200  # 幂等
    time.sleep(2.2)
    assert client.get(f"/api/jobs/{job_id}").json()["status"] == "cancelled"


def test_old_late_job_cannot_overwrite_newer_selection(client):
    """A 是旧选择（被取消），B 是新选择并已发布；
    A 若迟到完成，只能 superseded，页面仍展示 B。"""
    image = upload(client, make_png(768, 768)).json()["image"]

    crop_a = {"x": 0, "y": 0, "w": 200, "h": 200}
    crop_b = {"x": 300, "y": 300, "w": 300, "h": 300}

    ra = submit(client, image["id"], crop_a, delay_ms=2000)
    job_a = ra.json()["job"]["id"]

    wait_image(
        client, image["id"],
        lambda b: b["current_job"] is not None
        and b["current_job"]["status"] == "processing"
        and b["current_job"]["id"] == job_a,
    )

    # 用户改选 B：A 被取消，B 很快发布。
    cancel = client.post(f"/api/jobs/{job_a}/cancel")
    assert cancel.status_code == 200
    rb = submit(client, image["id"], crop_b, delay_ms=0)
    job_b = rb.json()["job"]["id"]

    done_b = wait_job(client, job_b)
    assert done_b["status"] == "published"

    # 等 A 的处理窗口彻底过去，确认 A 保持取消，当前版本始终是 B。
    time.sleep(2.5)
    final_a = client.get(f"/api/jobs/{job_a}").json()
    assert final_a["status"] == "cancelled", final_a
    state = client.get(f"/api/images/{image['id']}").json()
    assert state["current_version"]["id"] == done_b["version"]["id"]
    assert state["current_version"]["version_no"] == 1


def test_old_job_completing_after_new_selection_is_superseded(client):
    """构造 A 不取消、用户直接改选 B：A 跑完后必须 superseded，不能顶掉 B。"""
    image = upload(client, make_png(768, 768)).json()["image"]
    crop_a = {"x": 0, "y": 0, "w": 220, "h": 220}
    crop_b = {"x": 400, "y": 0, "w": 260, "h": 260}

    ra = submit(client, image["id"], crop_a, delay_ms=2500)
    job_a = ra.json()["job"]["id"]
    wait_image(
        client, image["id"],
        lambda b: b["current_job"] is not None
        and b["current_job"]["id"] == job_a
        and b["current_job"]["status"] == "processing",
    )

    # 直接提交 B（不取消 A），current_job_id 移到 B。
    rb = submit(client, image["id"], crop_b, delay_ms=0)
    job_b = rb.json()["job"]["id"]
    assert rb.status_code == 202
    done_b = wait_job(client, job_b)
    assert done_b["status"] == "published"

    # 等待 A 迟到完成。
    final_a = wait_job(client, job_a, timeout=10)
    assert final_a["status"] == "superseded", final_a
    assert final_a["version"]["id"] != done_b["version"]["id"]

    # 关键断言：页面当前版本仍是 B（旧响应未回写）。
    state = client.get(f"/api/images/{image['id']}").json()
    assert state["current_version"]["id"] == done_b["version"]["id"]

    # A 的派生成果仍然存在且可复用——再选 A 的参数立即返回它的版本。
    again = submit(client, image["id"], crop_a)
    assert again.status_code == 200
    assert again.json()["job"]["version"]["id"] == final_a["version"]["id"]
