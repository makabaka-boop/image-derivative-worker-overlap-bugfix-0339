from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from . import config

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS images (
    id              TEXT PRIMARY KEY,
    sha256          TEXT NOT NULL UNIQUE,
    format          TEXT NOT NULL,
    width           INTEGER NOT NULL,
    height          INTEGER NOT NULL,
    size_bytes      INTEGER NOT NULL,
    original_path   TEXT NOT NULL,
    current_version_id TEXT REFERENCES versions(id),
    current_job_id  TEXT REFERENCES jobs(id),
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    image_id    TEXT NOT NULL REFERENCES images(id),
    crop_x      INTEGER NOT NULL,
    crop_y      INTEGER NOT NULL,
    crop_w      INTEGER NOT NULL,
    crop_h      INTEGER NOT NULL,
    status      TEXT NOT NULL CHECK (status IN
                    ('pending','processing','published','cancelled','superseded','failed')),
    error       TEXT,
    version_id  TEXT REFERENCES versions(id),
    test_delay_ms INTEGER NOT NULL DEFAULT 0,
    worker_id   TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_image ON jobs(image_id);

CREATE TABLE IF NOT EXISTS versions (
    id          TEXT PRIMARY KEY,
    image_id    TEXT NOT NULL REFERENCES images(id),
    crop_x      INTEGER NOT NULL,
    crop_y      INTEGER NOT NULL,
    crop_w      INTEGER NOT NULL,
    crop_h      INTEGER NOT NULL,
    version_no  INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE (image_id, crop_x, crop_y, crop_w, crop_h)
);
CREATE INDEX IF NOT EXISTS idx_versions_image ON versions(image_id);

CREATE TABLE IF NOT EXISTS derivatives (
    id          TEXT PRIMARY KEY,
    image_id    TEXT NOT NULL REFERENCES images(id),
    size_px     INTEGER NOT NULL,
    key         TEXT NOT NULL UNIQUE,
    width       INTEGER NOT NULL,
    height      INTEGER NOT NULL,
    path        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS version_derivatives (
    version_id   TEXT NOT NULL REFERENCES versions(id),
    derivative_id TEXT NOT NULL REFERENCES derivatives(id),
    size_px      INTEGER NOT NULL,
    PRIMARY KEY (version_id, size_px)
);
"""


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path is not None else config.DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    conn = connect()
    try:
        # 建表顺序里 images 引用了 versions，需先关掉外键再执行整段脚本。
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.executescript(SCHEMA)
        conn.execute("PRAGMA foreign_keys=ON")
    finally:
        conn.close()


@contextmanager
def tx(db_path: Path | None = None, immediate: bool = True) -> Iterator[sqlite3.Connection]:
    """短事务上下文。默认 BEGIN IMMEDIATE，保证取消/发布等迁移串行化。"""
    conn = connect(db_path)
    try:
        if immediate:
            conn.execute("BEGIN IMMEDIATE")
        else:
            conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
