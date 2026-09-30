"""主流程、重复申请复用、参数改变产生新版本。"""
from __future__ import annotations

import io

from PIL import Image

from conftest import make_jpeg, make_png, submit, upload, wait_image, wait_job


def test_png_happy_path_generates_two_webp_sizes(client):
    r = upload(client, make_png(800, 600))
    assert r.status_code == 201, r.text
    image = r.json()["image"]
    assert image["width"] == 800 and image["height"] == 600
    assert image["current_version"] is None

    r = submit(client, image["id"], {"x": 100, "y": 50, "w": 400, "h": 300})
    assert r.status_code == 202, r.text
    job = r.json()["job"]
    assert job["status"] in ("pending", "processing")

    done = wait_job(client, job["id"])
    assert done["status"] == "published", done

    img = wait_image(
        client, image["id"],
        lambda b: b["current_version"] is not None
        and b["current_version"]["id"] == done["version"]["id"],
    )
    version = img["current_version"]
    assert version["version_no"] == 1
    assert version["crop"] == {"x": 100, "y": 50, "w": 400, "h": 300}
    sizes = sorted(d["size"] for d in version["derivatives"])
    assert sizes == [256, 512]

    # 两个派生图都可下载、确实是 WebP，且最长边不超目标、等比、不放大。
    box_ratio = 400 / 300
    for d in version["derivatives"]:
        rr = client.get(d["url"])
        assert rr.status_code == 200
        assert rr.headers["content-type"] == "image/webp"
        assert rr.content[:4] == b"RIFF" and rr.content[8:12] == b"WEBP"
        with Image.open(io.BytesIO(rr.content)) as out:
            assert out.format == "WEBP"
            w, h = out.size
            assert max(w, h) <= d["size"]
            assert abs((w / h) - box_ratio) < 0.02
            assert d["width"] == w and d["height"] == h
    big = next(d for d in version["derivatives"] if d["size"] == 512)
    assert (big["width"], big["height"]) == (512, 384)


def test_jpeg_happy_path(client):
    r = upload(client, make_jpeg(300, 300))
    assert r.status_code == 201, r.text
    image = r.json()["image"]
    r = submit(client, image["id"], {"x": 0, "y": 0, "w": 300, "h": 300})
    done = wait_job(client, r.json()["job"]["id"])
    assert done["status"] == "published"
    # 300x300：256 缩小、512 放大，最长边都精确等于目标。
    expected = {256: (256, 256), 512: (512, 512)}
    for d in done["version"]["derivatives"]:
        assert (d["width"], d["height"]) == expected[d["size"]]


def test_identical_upload_deduplicates(client):
    data = make_png(400, 200, (1, 2, 3, 255))
    r1 = upload(client, data)
    r2 = upload(client, data)
    assert r1.status_code == 201
    assert r2.status_code == 200
    assert r2.json()["reused"] is True
    assert r1.json()["image"]["id"] == r2.json()["image"]["id"]
    assert r1.json()["image"]["sha256"] == r2.json()["image"]["sha256"]


def test_duplicate_request_reuses_completed_version(client):
    image = upload(client, make_png(500, 500)).json()["image"]
    crop = {"x": 10, "y": 10, "w": 200, "h": 200}

    r1 = submit(client, image["id"], crop)
    first = wait_job(client, r1.json()["job"]["id"])
    assert first["status"] == "published"
    first_version_id = first["version"]["id"]

    # 相同原图摘要 + 相同裁切参数：立即复用，不再进 worker 队列。
    r2 = submit(client, image["id"], crop)
    assert r2.status_code == 200, r2.text
    body2 = r2.json()
    assert body2["reused"] is True
    assert body2["job"]["status"] == "published"
    assert body2["job"]["version"]["id"] == first_version_id
    # 复用的是同一个版本，而不是新建版本号。
    assert body2["job"]["version"]["version_no"] == 1


def test_concurrent_identical_requests_share_single_job(client):
    import concurrent.futures

    image = upload(client, make_png(640, 480)).json()["image"]
    crop = {"x": 0, "y": 0, "w": 300, "h": 300}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(
            pool.map(lambda _: submit(client, image["id"], crop), range(4))
        )
    jobs = [r.json()["job"] for r in responses]
    job_ids = {j["id"] for j in jobs}
    # 串行化的事务保证只产生一个在途作业。
    assert len(job_ids) == 1, jobs
    only = wait_job(client, next(iter(job_ids)))
    assert only["status"] == "published"

    # 之后再请求仍复用同一版本。
    again = submit(client, image["id"], crop)
    assert again.status_code == 200
    assert again.json()["job"]["version"]["id"] == only["version"]["id"]


def test_changed_crop_creates_new_version_and_old_remains(client):
    image = upload(client, make_png(700, 700)).json()["image"]

    r1 = submit(client, image["id"], {"x": 0, "y": 0, "w": 200, "h": 200})
    v1 = wait_job(client, r1.json()["job"]["id"])["version"]
    assert v1["version_no"] == 1

    # 参数改变 → 新版本。
    r2 = submit(client, image["id"], {"x": 100, "y": 100, "w": 300, "h": 300})
    assert r2.status_code == 202
    v2 = wait_job(client, r2.json()["job"]["id"])["version"]
    assert v2["version_no"] == 2
    assert v2["id"] != v1["id"]

    state = client.get(f"/api/images/{image['id']}").json()
    assert state["current_version"]["id"] == v2["id"]

    # 切回旧参数：旧版本仍可复用，版本号保持 2（不会新建 v3）。
    r3 = submit(client, image["id"], {"x": 0, "y": 0, "w": 200, "h": 200})
    assert r3.status_code == 200
    assert r3.json()["job"]["version"]["id"] == v1["id"]
    assert r3.json()["job"]["version"]["version_no"] == 1
    state = client.get(f"/api/images/{image['id']}").json()
    assert state["current_version"]["id"] == v1["id"]
