"""坏图、体积限制与裁切越界。"""
from __future__ import annotations

from conftest import make_png, submit, upload


def test_garbage_bytes_rejected(client):
    r = upload(client, b"this is absolutely not an image", "image/png")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "bad_request"


def test_empty_file_rejected(client):
    r = upload(client, b"", "image/png")
    assert r.status_code == 400


def test_disguised_html_rejected(client):
    payload = b"<html><body>nope</body></html>"
    r = upload(client, payload, "image/png")
    assert r.status_code == 400


def test_wrong_declared_content_type_rejected(client):
    # 实际是 PNG，但声明成 text/plain。
    r = upload(client, make_png(10, 10), "text/plain")
    assert r.status_code == 400


def test_gif_not_allowed_even_though_decodeable(client):
    import io
    from PIL import Image
    buf = io.BytesIO()
    Image.new("P", (8, 8), 0).save(buf, format="GIF")
    r = upload(client, buf.getvalue(), "image/png")
    assert r.status_code == 400


def test_oversize_upload_rejected(client):
    # 5 MiB 随机噪声 PNG（压缩后仍远超 4 MiB 限制）。
    import os
    r = upload(client, os.urandom(5 * 1024 * 1024), "image/png")
    assert r.status_code in (400, 413)
    assert r.status_code == 413


def test_413_is_returned_for_file_over_4mib(client):
    big = b"\x89PNG\r\n\x1a\n" + b"\x00" * (4 * 1024 * 1024 + 10)
    r = upload(client, big, "image/png")
    assert r.status_code == 413


def test_crop_out_of_bounds_rejected(client):
    image = upload(client, make_png(300, 200)).json()["image"]
    base = {"x": 0, "y": 0, "w": 100, "h": 100}
    bad_crops = [
        {"x": 250, "y": 0, "w": 100, "h": 100},    # x + w > width
        {"x": 0, "y": 150, "w": 100, "h": 100},    # y + h > height
        {"x": -1, "y": 0, "w": 100, "h": 100},     # 负起点
        {"x": 0, "y": 0, "w": 0, "h": 100},        # 零宽
        {"x": 0, "y": 0, "w": 100, "h": -5},       # 负高
        {"x": 0, "y": 0, "w": 301, "h": 200},      # 宽超出
    ]
    for bad in bad_crops:
        r = submit(client, image["id"], bad)
        assert r.status_code == 400, f"应拒绝裁切 {bad}: {r.status_code} {r.text}"
        assert r.json()["error"]["code"] == "bad_request"


def test_crop_exactly_at_edge_is_allowed(client):
    image = upload(client, make_png(300, 200)).json()["image"]
    r = submit(client, image["id"], {"x": 100, "y": 0, "w": 200, "h": 200})
    assert r.status_code == 202, r.text


def test_crop_non_integer_rejected(client):
    image = upload(client, make_png(100, 100)).json()["image"]
    r = submit(client, image["id"], {"x": 0.5, "y": 0, "w": 10, "h": 10})
    assert r.status_code == 422


def test_submit_for_missing_image_404(client):
    r = submit(client, "deadbeef", {"x": 0, "y": 0, "w": 10, "h": 10})
    assert r.status_code == 404
