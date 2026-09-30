from __future__ import annotations

import io
import os
import time

import httpx
import pytest
from PIL import Image

BASE_URL = os.environ.get("BASE_URL", "http://api:8000")

TERMINAL = {"published", "cancelled", "superseded", "failed"}


@pytest.fixture(scope="session")
def base_url():
    return BASE_URL


@pytest.fixture(scope="session")
def client():
    deadline = time.time() + 40
    last_exc = None
    while time.time() < deadline:
        try:
            with httpx.Client(base_url=BASE_URL, timeout=httpx.Timeout(30)) as c:
                r = c.get("/api/health")
                if r.status_code == 200:
                    # 会话开始时清空服务端状态，保证 verify 可重复执行。
                    rr = c.post("/api/_test/reset")
                    assert rr.status_code == 200, rr.text
                    yield c
                    return
        except httpx.TransportError as exc:
            last_exc = exc
        time.sleep(0.5)
    raise RuntimeError(f"API 未就绪: {BASE_URL}: {last_exc}")


def make_png(width: int, height: int, color=(220, 30, 30, 255)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGBA", (width, height), color).save(buf, format="PNG")
    return buf.getvalue()


def make_jpeg(width: int, height: int, color=(30, 120, 220)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), color).save(buf, format="JPEG", quality=90)
    return buf.getvalue()


def upload(client, data: bytes, content_type: str = "image/png"):
    files = {"file": ("img", data, content_type)}
    return client.post("/api/images", files=files)


def submit(client, image_id: str, crop: dict, delay_ms: int | None = None):
    headers = {}
    if delay_ms is not None:
        headers["X-Test-Delay-Ms"] = str(delay_ms)
    return client.post(
        f"/api/images/{image_id}/jobs", json=crop, headers=headers
    )


def wait_job(client, job_id: str, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    while True:
        r = client.get(f"/api/jobs/{job_id}")
        assert r.status_code == 200, r.text
        body = r.json()
        if body["status"] in TERMINAL:
            return body
        if time.time() > deadline:
            raise AssertionError(f"作业 {job_id} 未在 {timeout}s 内结束: {body['status']}")
        time.sleep(0.1)


def wait_image(client, image_id: str, predicate, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    last = None
    while True:
        r = client.get(f"/api/images/{image_id}")
        assert r.status_code == 200, r.text
        last = r.json()
        if predicate(last):
            return last
        if time.time() > deadline:
            raise AssertionError(f"图片状态未满足条件: {last}")
        time.sleep(0.1)
