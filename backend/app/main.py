from __future__ import annotations

from fastapi import FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, StrictInt

from . import config, service

app = FastAPI(title="图片裁切派生服务", version="1.0.0")


@app.on_event("startup")
def _startup() -> None:
    service.startup()


@app.exception_handler(service.ServiceError)
async def _service_error_handler(request: Request, exc: service.ServiceError):
    return JSONResponse(
        status_code=exc.status,
        content={"error": {"code": exc.code, "message": str(exc)}},
    )


@app.exception_handler(ValueError)
async def _value_error_handler(request: Request, exc: ValueError):
    return JSONResponse(
        status_code=400,
        content={"error": {"code": "bad_request", "message": str(exc)}},
    )


class CropIn(BaseModel):
    x: StrictInt
    y: StrictInt
    w: StrictInt
    h: StrictInt


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/_test/reset")
def test_reset():
    """验收测试隔离用：只有显式启用 TEST_HOOKS 时可调用。"""
    if not config.TEST_HOOKS_ENABLED:
        raise HTTPException(status_code=404, detail="not found")
    service.reset_all_for_tests()
    return {"status": "reset"}


@app.post("/api/images")
async def upload_image(file: UploadFile = File(...)):
    data = await file.read(config.MAX_UPLOAD_BYTES + 1)
    if len(data) > config.MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="文件超过 4 MiB 限制")
    result = service.save_upload(data, file.content_type or "")
    return JSONResponse(
        status_code=201 if not result["reused"] else 200,
        content={"reused": result["reused"], "image": result["image"]},
    )


@app.get("/api/images")
def images_list():
    return {"images": service.list_images()}


@app.get("/api/images/{image_id}")
def image_detail(image_id: str):
    return service.get_image(image_id)


@app.get("/api/images/{image_id}/original")
def image_original(image_id: str):
    info = service.get_original_file(image_id)
    return FileResponse(
        info["path"],
        media_type="image/png" if info["format"] == "PNG" else "image/jpeg",
        headers={"Cache-Control": "private, max-age=3600"},
    )


@app.post("/api/images/{image_id}/jobs", status_code=202)
def create_job(
    image_id: str,
    crop: CropIn,
    x_test_delay_ms: int | None = Header(default=None, alias="X-Test-Delay-Ms"),
):
    delay = 0
    if x_test_delay_ms is not None:
        if not config.TEST_HOOKS_ENABLED:
            raise HTTPException(status_code=400, detail="测试钩子未启用")
        if not (0 <= x_test_delay_ms <= config.MAX_TEST_DELAY_MS):
            raise HTTPException(status_code=400, detail="延迟参数超出范围")
        delay = x_test_delay_ms
    result = service.submit_job(
        image_id, (crop.x, crop.y, crop.w, crop.h), test_delay_ms=delay
    )
    return JSONResponse(
        status_code=result["http_status"],
        content={"reused": result["reused"], "job": result["job"]},
    )


@app.get("/api/jobs/{job_id}")
def job_detail(job_id: str):
    return service.get_job(job_id)


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    result = service.cancel_job(job_id)
    return {"changed": result["changed"], "job": result["job"]}


@app.get("/api/derivatives/{deriv_id}")
def derivative_content(deriv_id: str):
    info = service.get_derivative(deriv_id)
    # 内容寻址且不可变：可以激进缓存。
    return FileResponse(
        info["path"],
        media_type="image/webp",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )
