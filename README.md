# 图片裁切派生服务

上传不超过 **4 MiB** 的 PNG/JPEG，在页面上框选裁切区域，由后端生成 **256 / 512 像素**
两档 WebP 派生图。前端 React + Vite，后端 FastAPI，持久层 SQLite，渲染由独立
worker 进程完成；三者通过 Docker Compose 编排。

## 快速开始

```bash
# 1. 校验 compose 文件
docker compose config --quiet

# 2. 构建镜像（api/worker/web/verify 全部在本地构建）
docker compose build

# 3. 运行一次性验收（需要的 api 与 worker 会一并启动）
docker compose run --rm verify
```

验收通过后启动整套服务并打开浏览器：

```bash
docker compose up -d
# 页面：     http://localhost:8080
# API 文档： http://localhost:8000/docs
```

停止：`docker compose down -v`（`-v` 一并清空数据卷）。

## 目录结构

```
backend/
  app/
    config.py    常量（4 MiB、256/512、路径、测试钩子）
    db.py        SQLite 连接、schema、BEGIN IMMEDIATE 短事务
    storage.py   原图/派生图内容寻址落盘、临时文件暂存与启动清理
    imaging.py   图片解码校验、裁切合法性、等比 WebP 渲染
    service.py   业务核心：上传去重、作业申请、取消、领取、一次性发布
    main.py      FastAPI 路由
    worker.py    轮询/并发执行循环、信号处理、崩溃恢复
  tests/         pytest 验收用例（HTTP 黑盒 + 崩溃恢复子进程）
  Dockerfile
web/
  src/App.jsx    上传、canvas 裁切框、作业轮询、版本展示
  Dockerfile     多阶段构建（node 构建 + nginx 反代 /api）
docker-compose.yml
```

## 数据模型

- `images`：原图，按 **sha256 内容寻址**，重复上传只产生一条记录。
- `versions`：对 `(image_id, x, y, w, h)` 唯一。相同原图摘要 + 相同裁切参数
  永远复用同一个版本；参数变化才产生递增 `version_no` 的新版本。
- `derivatives`：单张 WebP 派生图，按 `sha256(原图摘要|裁切框|尺寸)` 内容寻址，
  跨版本/跨作业共享。
- `version_derivatives`：版本 ↔ 256/512 两张派生图（每档唯一）。
- `jobs`：一次申请。状态机
  `pending → processing → published | cancelled | superseded | failed`。
- `images.current_version_id`：页面唯一展示依据；`images.current_job_id`：当前选择。

## 关键语义与实现

### 去重与新版本

申请作业（`POST /api/images/{id}/jobs`）在同一 `BEGIN IMMEDIATE` 事务内：

1. 相同 `(原图摘要, 裁切参数)` 已有版本 → 直接生成一条 `published` 作业并指回该
   版本（HTTP 200），不进 worker 队列；
2. 否则同参数已有 `pending/processing` 作业 → 复用该作业（HTTP 202）；
3. 否则新建 pending 作业（HTTP 202），成为 `current_job_id`；旧已发布版本保留展示。

### 两尺寸原子发布

worker 把两张 WebP 先写入 `tmp/`（`*.staging-<job>-<size>`），**两个尺寸都成功**
后才在同一个数据库事务里：建版本、登记派生图、把临时文件 `os.replace` 到内容
寻址目标、按守卫更新当前版本指针。任一尺寸失败则作业 `failed`，不会出现只有
一个尺寸的半成品版本。

### 取消/完成只有一个终态

取消与发布都用 `BEGIN IMMEDIATE` 串行化，并附带条件更新
`WHERE status IN ('pending','processing')`：

- 取消先提交 → 发布事务读到的状态已不是 processing，丢弃成果，作业保持
  `cancelled`，临时文件清理；
- 发布先提交 → 取消得到 409，作业保持 `published`。对已取消作业重复取消幂等。

### 旧作业迟到完成不能覆盖当前选择

发布事务更新当前版本时带守卫：

```sql
UPDATE images SET current_version_id=?
 WHERE id=? AND current_job_id=?      -- 仍是当前选择才允许发布
```

守卫不命中说明用户已经改选，作业终态记为 `superseded`，其渲染成果照常落库、
以后选回该参数可立即复用，但页面当前版本不变。前端另外用 `currentJobId`
丢弃迟到的旧轮询响应，双重防止旧响应回写。

### 崩溃重启恢复

worker 启动时先清空 `tmp/`，再把所有 `processing` 作业无条件退回 `pending`，
随后正常认领重试。派生图内容寻址 + `INSERT OR IGNORE` 使重试安全：
崩溃前已落正式位置的文件会被复用，数据库里不会出现重复行。

### 输入校验

- 上传：≤ 4 MiB（超限 413）；声明 MIME 必须是 `image/png`/`image/jpeg`；
  字节必须能被 Pillow 实际解码且格式一致，伪装的 HTML/GIF/截断图一律 400。
- 裁切：整数、宽高为正、起点非负、`x+w ≤ width`、`y+h ≤ height`，越界 400；
  恰好贴边允许。
- 输出：裁切框等比缩放到最长边恰为 256/512（小图允许放大），WebP quality 85。

## HTTP 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/images` | multipart 上传，重复内容 200，新内容 201 |
| GET  | `/api/images/{id}` | 当前已发布版本 + 当前作业（页面状态） |
| GET  | `/api/images/{id}/original` | 原图预览 |
| POST | `/api/images/{id}/jobs` | 申请裁切；200=复用版本，202=新建/复用在途作业 |
| GET  | `/api/jobs/{id}` | 作业状态轮询 |
| POST | `/api/jobs/{id}/cancel` | 取消（终态冲突 409，重复取消幂等） |
| GET  | `/api/derivatives/{id}` | WebP 文件（不可变，长缓存） |

## 测试覆盖

`docker compose run --rm verify` 在一次性容器里对完整 api+worker 栈执行
pytest（约 30 秒）：

- **重复申请**：相同上传去重、相同参数复用版本、并发 4 个相同申请只产生一个
  在途作业、参数改变产生 v2、切回旧参数复用 v1；
- **坏图**：随机字节、空文件、伪装 HTML、MIME 不符、GIF、超过 4 MiB；
- **裁切越界**：负坐标/零宽/越界等 6 种非法框拒绝，贴边框允许，非整数 422；
- **取消竞态**：取消先于完成（保持 cancelled）、完成后取消（409）、重复取消幂等；
- **旧响应回写**：旧作业取消 + 新作业发布后展示新版本；旧作业不取消、迟到完成
  时终态为 `superseded`，页面仍展示新版本，其成果可再复用；
- **重启恢复**：作业渲染中途 `SIGKILL` worker，重启后清理临时文件、重置
  processing、重试并发布两尺寸，派生图可下载。

> 测试依赖 `X-Test-Delay-Ms` 请求头注入人工渲染延迟，以及会话开始时的
> `POST /api/_test/reset`；两者仅在 `TEST_HOOKS=1` 时启用（compose 默认开启，
> 生产环境把该环境变量删掉即关闭，重置端点返回 404）。
