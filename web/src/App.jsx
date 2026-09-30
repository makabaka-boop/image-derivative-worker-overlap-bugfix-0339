import { useCallback, useEffect, useRef, useState } from 'react'

const MAX_BYTES = 4 * 1024 * 1024

const STATUS_LABEL = {
  pending: '排队中',
  processing: '处理中',
  published: '已发布',
  cancelled: '已取消',
  superseded: '已被新版本取代',
  failed: '失败',
}

async function api(path, options) {
  const res = await fetch(path, options)
  let body = null
  try {
    body = await res.json()
  } catch {
    /* 非 JSON 响应 */
  }
  if (!res.ok) {
    const message = body?.error?.message || body?.detail || `请求失败 (${res.status})`
    throw new Error(message)
  }
  return body
}

export default function App() {
  const [image, setImage] = useState(null)
  const [job, setJob] = useState(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  // 当前选择的作业 id：用于丢弃迟到的旧轮询响应，防止旧响应回写当前视图。
  const currentJobId = useRef(null)
  const pollTimer = useRef(null)

  const refreshImage = useCallback(async (imageId) => {
    const fresh = await api(`/api/images/${imageId}`)
    setImage(fresh)
    if (fresh.current_job) {
      const j = await api(`/api/jobs/${fresh.current_job.id}`)
      // 只接受当前选择的作业；旧作业完成不得回写覆盖。
      if (j.id === currentJobId.current) setJob(j)
    }
    return fresh
  }, [])

  // 当存在未终态的当前作业时轮询。
  useEffect(() => {
    if (!image?.current_job) return
    const terminal = ['published', 'cancelled', 'superseded', 'failed']
    const isRunning = !terminal.includes(image.current_job.status)
    if (!isRunning) return

    pollTimer.current = setInterval(async () => {
      try {
        await refreshImage(image.id)
      } catch (e) {
        // 单次轮询失败忽略，下一拍重试。
        console.warn(e)
      }
    }, 600)
    return () => clearInterval(pollTimer.current)
  }, [image?.id, image?.current_job?.id, image?.current_job?.status, refreshImage])

  const onUpload = async (event) => {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) return
    if (file.size > MAX_BYTES) {
      setError('文件超过 4 MiB 限制')
      return
    }
    if (!['image/png', 'image/jpeg'].includes(file.type)) {
      setError('仅支持 PNG / JPEG')
      return
    }
    setError('')
    setBusy(true)
    setImage(null)
    setJob(null)
    currentJobId.current = null
    try {
      const form = new FormData()
      form.append('file', file)
      const body = await api('/api/images', { method: 'POST', body: form })
      setImage(body.image)
    } catch (e) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  const onSubmit = async (crop) => {
    if (!image) return
    setError('')
    setBusy(true)
    try {
      const body = await api(`/api/images/${image.id}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(crop),
      })
      currentJobId.current = body.job.id
      setJob(body.job)
      await refreshImage(image.id)
    } catch (e) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  const onCancel = async () => {
    if (!job) return
    setBusy(true)
    try {
      await api(`/api/jobs/${job.id}/cancel`, { method: 'POST' })
      await refreshImage(image.id)
    } catch (e) {
      setError(e.message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="page">
      <header>
        <h1>图片裁切派生</h1>
        <p className="hint">
          上传不超过 4 MiB 的 PNG/JPEG，选择裁切框，服务端生成 256 / 512 像素 WebP。
          相同原图摘要与裁切参数会复用已完成结果；页面只展示已确认发布的版本。
        </p>
      </header>

      <section className="upload">
        <input type="file" accept="image/png,image/jpeg" onChange={onUpload} />
        {busy && <span className="tag">处理中…</span>}
      </section>

      {error && <div className="error">{error}</div>}

      {image && (
        <Cropper
          key={image.id}
          imageId={image.id}
          width={image.width}
          height={image.height}
          disabled={busy}
          onSubmit={onSubmit}
        />
      )}

      {job && (
        <section className="job">
          <h2>当前作业</h2>
          <p>
            状态：<strong>{STATUS_LABEL[job.status] || job.status}</strong>
            {job.crop && (
              <span className="crop-info">
                {' '}
                裁切 ({job.crop.x}, {job.crop.y}, {job.crop.w}×{job.crop.h})
              </span>
            )}
          </p>
          {job.status === 'failed' && <div className="error">{job.error}</div>}
          {['pending', 'processing'].includes(job.status) && (
            <button onClick={onCancel} disabled={busy}>
              取消作业
            </button>
          )}
        </section>
      )}

      {image?.current_version ? (
        <section className="result">
          <h2>已发布版本 v{image.current_version.version_no}</h2>
          <p className="crop-info">
            裁切框：x={image.current_version.crop.x}, y={image.current_version.crop.y},
            w={image.current_version.crop.w}, h={image.current_version.crop.h}
          </p>
          <div className="thumbs">
            {image.current_version.derivatives.map((d) => (
              <figure key={d.id}>
                <img src={d.url} alt={`WebP ${d.size}px`} />
                <figcaption>
                  目标 {d.size}px · 实际 {d.width}×{d.height} · WebP
                </figcaption>
              </figure>
            ))}
          </div>
        </section>
      ) : (
        image && <p className="hint">尚无已发布版本，提交一个裁切框开始。</p>
      )}
    </div>
  )
}

function Cropper({ imageId, width, height, disabled, onSubmit }) {
  const canvasRef = useRef(null)
  const imgRef = useRef(null)
  const [scale, setScale] = useState(1)
  const [sel, setSel] = useState(null)
  const dragStart = useRef(null)

  const draw = useCallback(() => {
    const canvas = canvasRef.current
    const img = imgRef.current
    if (!canvas || !img) return
    const maxW = Math.min(720, window.innerWidth - 64)
    const s = Math.min(1, maxW / width)
    setScale(s)
    canvas.width = Math.round(width * s)
    canvas.height = Math.round(height * s)
    const ctx = canvas.getContext('2d')
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height)
  }, [width, height])

  const paintSelection = (rect) => {
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    ctx.drawImage(imgRef.current, 0, 0, canvas.width, canvas.height)
    if (!rect) return
    ctx.fillStyle = 'rgba(0, 102, 255, 0.22)'
    ctx.strokeStyle = '#0066ff'
    ctx.lineWidth = 2
    ctx.fillRect(rect.x, rect.y, rect.w, rect.h)
    ctx.strokeRect(rect.x + 1, rect.y + 1, rect.w - 2, rect.h - 2)
  }

  useEffect(() => {
    const img = new Image()
    img.onload = () => {
      imgRef.current = img
      draw()
    }
    img.src = `/api/images/${imageId}/original`
  }, [imageId, draw])

  const eventPos = (event) => {
    const rect = canvasRef.current.getBoundingClientRect()
    return {
      x: Math.max(0, Math.min(canvasRef.current.width, event.clientX - rect.left)),
      y: Math.max(0, Math.min(canvasRef.current.height, event.clientY - rect.top)),
    }
  }

  const onPointerDown = (event) => {
    if (disabled) return
    canvasRef.current.setPointerCapture(event.pointerId)
    dragStart.current = eventPos(event)
  }

  const onPointerMove = (event) => {
    if (!dragStart.current) return
    const p = eventPos(event)
    const rect = {
      x: Math.min(dragStart.current.x, p.x),
      y: Math.min(dragStart.current.y, p.y),
      w: Math.abs(p.x - dragStart.current.x),
      h: Math.abs(p.y - dragStart.current.y),
    }
    setSel(rect)
    paintSelection(rect)
  }

  const onPointerUp = () => {
    dragStart.current = null
  }

  const submit = () => {
    if (!sel || sel.w < 2 || sel.h < 2) return
    onSubmit({
      x: Math.round(sel.x / scale),
      y: Math.round(sel.y / scale),
      w: Math.round(sel.w / scale),
      h: Math.round(sel.h / scale),
    })
  }

  return (
    <section className="cropper">
      <h2>选择裁切框</h2>
      <canvas
        ref={canvasRef}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
      />
      <div className="actions">
        <button onClick={submit} disabled={disabled || !sel || sel.w < 2 || sel.h < 2}>
          提交裁切（生成 256 / 512 WebP）
        </button>
        {sel && (
          <button
            onClick={() => {
              setSel(null)
              paintSelection(null)
            }}
            disabled={disabled}
          >
            清除选择
          </button>
        )}
      </div>
    </section>
  )
}
