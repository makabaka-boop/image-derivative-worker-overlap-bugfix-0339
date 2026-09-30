from __future__ import annotations

import io
from dataclasses import dataclass

from PIL import Image, UnidentifiedImageError

from . import config


@dataclass
class ImageInfo:
    fmt: str
    width: int
    height: int


def validate(data: bytes, declared: str) -> ImageInfo:
    """同时校验声明类型与实际内容，返回图片信息。坏图抛 ValueError。"""
    if declared not in config.ALLOWED_CONTENT_TYPES:
        raise ValueError("仅支持 PNG/JPEG 图片")
    try:
        with Image.open(io.BytesIO(data)) as im:
            fmt = im.format
            if fmt not in config.ALLOWED_FORMATS:
                raise ValueError("图片实际格式不是 PNG/JPEG")
            # load() 会真正解码，能发现截断等损坏。
            im.load()
            width, height = im.size
    except (UnidentifiedImageError, OSError, ValueError) as exc:
        if isinstance(exc, ValueError) and "实际格式" in str(exc):
            raise
        raise ValueError("无法解码的图片文件") from exc
    return ImageInfo(fmt=fmt, width=width, height=height)


def validate_crop(info: ImageInfo, x: int, y: int, w: int, h: int) -> None:
    for name, val in (("x", x), ("y", y), ("w", w), ("h", h)):
        if not isinstance(val, int) or isinstance(val, bool):
            raise ValueError(f"裁切参数 {name} 必须是整数")
    if w <= 0 or h <= 0:
        raise ValueError("裁切宽高必须为正数")
    if x < 0 or y < 0:
        raise ValueError("裁切起点不能为负数")
    if x + w > info.width or y + h > info.height:
        raise ValueError("裁切区域超出原图范围")


def render_webp(
    data: bytes,
    fmt: str,
    box: tuple[int, int, int, int],
    target: int,
) -> tuple[bytes, int, int]:
    """按裁切框生成最长边恰为 target 的等比 WebP（允许放大），返回(字节, 宽, 高)。"""
    with Image.open(io.BytesIO(data)) as im:
        im.load()
        if fmt == "JPEG":
            im = im.convert("RGB")
        cropped = im.crop((box[0], box[1], box[0] + box[2], box[1] + box[3]))
        cw, ch = cropped.size
        scale = target / max(cw, ch)
        out_w = max(1, round(cw * scale))
        out_h = max(1, round(ch * scale))
        if (out_w, out_h) != (cw, ch):
            cropped = cropped.resize((out_w, out_h), Image.LANCZOS)
        buf = io.BytesIO()
        cropped.save(buf, format="WEBP", quality=config.WEBP_QUALITY, method=4)
        return buf.getvalue(), out_w, out_h
