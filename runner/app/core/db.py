"""
Runner database utilities.

Responsibilities:
- define Runner SQLite schema
- initialize database on startup
- provide DB connections
- provide lightweight migration helpers
- enable foreign keys and Row access

This DB stores platform-level truth for Runner:
- runs
- run_outputs
- forecast_registry
- scheduled_tasks
- cleanup_log
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from app.core.settings import RUNNER_DB_PATH


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def now_iso() -> str:
    """
    Return current UTC time in ISO 8601 format.
    """
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------
# Main schema
# ---------------------------------------------------------------------
DDL = """
PRAGMA foreign_keys = ON;

-- =========================================================
-- runs
-- Platform-level truth for every submitted run/job.
-- =========================================================
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    user_id INTEGER,
    username TEXT NOT NULL DEFAULT '',
    site_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    task_type TEXT NOT NULL,
    trigger_type TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL DEFAULT 'queued',
    payload_json TEXT NOT NULL DEFAULT '{}',
    output_dir TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL,
    heartbeat_at TEXT,
    error_message TEXT NOT NULL DEFAULT '',
    retention_class TEXT NOT NULL DEFAULT 'normal',
    site_base_url TEXT NOT NULL DEFAULT '',
    scheduled_task_id INTEGER,
    cleanup_status TEXT NOT NULL DEFAULT '',
    cleaned_at TEXT,

    CHECK (task_type IN ('simulate', 'custom', 'auto_forecast', 'mcmc', 'forecast')),
    CHECK (trigger_type IN ('manual', 'scheduled', 'system')),
    CHECK (status IN ('queued', 'running', 'done', 'failed', 'cancelled')),
    CHECK (retention_class IN ('ephemeral', 'normal', 'published'))
);

CREATE INDEX IF NOT EXISTS idx_runs_site_created
ON runs(site_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_runs_user_created
ON runs(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_runs_status
ON runs(status);

CREATE INDEX IF NOT EXISTS idx_runs_task_type
ON runs(task_type);

CREATE INDEX IF NOT EXISTS idx_runs_retention
ON runs(retention_class);

CREATE INDEX IF NOT EXISTS idx_runs_scheduled_task
ON runs(scheduled_task_id, created_at DESC);


-- =========================================================
-- run_outputs
-- Artifact registry for each run.
-- Runner stores metadata, while site owns raw files.
-- =========================================================
CREATE TABLE IF NOT EXISTS run_outputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    model_id TEXT NOT NULL DEFAULT '',
    variable TEXT NOT NULL DEFAULT '',
    treatment TEXT NOT NULL DEFAULT '',
    path TEXT NOT NULL DEFAULT '',
    rel_path TEXT NOT NULL DEFAULT '',
    media_type TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,

    FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_run_outputs_run
ON run_outputs(run_id);

CREATE INDEX IF NOT EXISTS idx_run_outputs_artifact
ON run_outputs(artifact_type);

CREATE INDEX IF NOT EXISTS idx_run_outputs_model
ON run_outputs(model_id);

CREATE INDEX IF NOT EXISTS idx_run_outputs_var_trt
ON run_outputs(variable, treatment);

CREATE INDEX IF NOT EXISTS idx_run_outputs_run_artifact_model
ON run_outputs(run_id, artifact_type, model_id);


-- =========================================================
-- forecast_registry
-- Published/latest forecast index for Forecast page.
-- This is the truth source for published forecast versions.
-- =========================================================
CREATE TABLE IF NOT EXISTS forecast_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    variable TEXT NOT NULL,
    treatment TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    data_path TEXT NOT NULL DEFAULT '',
    obs_path TEXT NOT NULL DEFAULT '',
    source_ref_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL,
    is_latest INTEGER NOT NULL DEFAULT 1,
    is_published INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CHECK (is_latest IN (0, 1)),
    CHECK (is_published IN (0, 1)),

    FOREIGN KEY(source_run_id) REFERENCES runs(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_lookup
ON forecast_registry(site_id, model_id, variable, treatment, is_latest);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_run
ON forecast_registry(source_run_id);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_updated
ON forecast_registry(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_pub_latest
ON forecast_registry(site_id, is_latest, is_published, updated_at DESC);


-- =========================================================
-- scheduled_tasks
-- Recurring auto forecast plans.
-- One schedule row can produce many runs over time.
-- =========================================================
CREATE TABLE IF NOT EXISTS scheduled_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    created_by_user_id INTEGER,
    created_by_username TEXT NOT NULL DEFAULT '',
    task_type TEXT NOT NULL DEFAULT 'auto_forecast',
    enabled INTEGER NOT NULL DEFAULT 1,
    cron_expr TEXT NOT NULL DEFAULT '',
    config_json TEXT NOT NULL DEFAULT '{}',
    last_run_at TEXT,
    next_run_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_run_id TEXT NOT NULL DEFAULT '',
    last_run_status TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    last_triggered_at TEXT,

    CHECK (task_type IN ('auto_forecast')),
    CHECK (enabled IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_site
ON scheduled_tasks(site_id, enabled);

CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_next
ON scheduled_tasks(enabled, next_run_at);


-- =========================================================
-- cleanup_log
-- Audit trail for cleanup actions.
-- =========================================================
CREATE TABLE IF NOT EXISTS cleanup_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    target_path TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cleanup_log_run
ON cleanup_log(run_id);

CREATE INDEX IF NOT EXISTS idx_cleanup_log_created
ON cleanup_log(created_at DESC);
"""


# ---------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------
async def get_db() -> aiosqlite.Connection:
    """
    Open a SQLite connection for Runner.

    Notes:
    - row_factory is enabled so callers can access columns by name
    - foreign keys are enabled for every connection
    """
    db_path = Path(RUNNER_DB_PATH)
    if db_path.parent and not db_path.parent.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)

    db = await aiosqlite.connect(str(db_path))
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys = ON;")
    return db


# ---------------------------------------------------------------------
# Lightweight migration helpers
# ---------------------------------------------------------------------
async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    """
    Return True if a table exists in the current SQLite database.
    """
    cur = await db.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table,),
    )
    row = await cur.fetchone()
    return row is not None


async def _ensure_column(
    db: aiosqlite.Connection,
    table: str,
    column: str,
    ddl_add: str,
) -> None:
    """
    Add a column if it does not already exist.
    """
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    cols = [r["name"] if isinstance(r, aiosqlite.Row) else r[1] for r in rows]
    if column not in cols:
        await db.execute(ddl_add)


async def migrate_db(db: aiosqlite.Connection) -> None:
    """
    Apply lightweight migrations for older runner.db files.

    Notes:
    - This function is intentionally additive only.
    - It avoids destructive schema rewrites.
    - It is safe to call on every startup.
    """
    if await _table_exists(db, "runs"):
        await _ensure_column(
            db,
            table="runs",
            column="heartbeat_at",
            ddl_add="ALTER TABLE runs ADD COLUMN heartbeat_at TEXT",
        )
        await _ensure_column(
            db,
            table="runs",
            column="retention_class",
            ddl_add="ALTER TABLE runs ADD COLUMN retention_class TEXT NOT NULL DEFAULT 'normal'",
        )
        await _ensure_column(
            db,
            table="runs",
            column="site_base_url",
            ddl_add="ALTER TABLE runs ADD COLUMN site_base_url TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="runs",
            column="scheduled_task_id",
            ddl_add="ALTER TABLE runs ADD COLUMN scheduled_task_id INTEGER",
        )
        await _ensure_column(
            db,
            table="runs",
            column="cleanup_status",
            ddl_add="ALTER TABLE runs ADD COLUMN cleanup_status TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="runs",
            column="cleaned_at",
            ddl_add="ALTER TABLE runs ADD COLUMN cleaned_at TEXT",
        )

    if await _table_exists(db, "run_outputs"):
        await _ensure_column(
            db,
            table="run_outputs",
            column="model_id",
            ddl_add="ALTER TABLE run_outputs ADD COLUMN model_id TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="run_outputs",
            column="rel_path",
            ddl_add="ALTER TABLE run_outputs ADD COLUMN rel_path TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="run_outputs",
            column="media_type",
            ddl_add="ALTER TABLE run_outputs ADD COLUMN media_type TEXT NOT NULL DEFAULT ''",
        )

    if await _table_exists(db, "forecast_registry"):
        await _ensure_column(
            db,
            table="forecast_registry",
            column="data_path",
            ddl_add="ALTER TABLE forecast_registry ADD COLUMN data_path TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="forecast_registry",
            column="obs_path",
            ddl_add="ALTER TABLE forecast_registry ADD COLUMN obs_path TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="forecast_registry",
            column="source_ref_json",
            ddl_add="ALTER TABLE forecast_registry ADD COLUMN source_ref_json TEXT NOT NULL DEFAULT '{}'",
        )
        await _ensure_column(
            db,
            table="forecast_registry",
            column="is_latest",
            ddl_add="ALTER TABLE forecast_registry ADD COLUMN is_latest INTEGER NOT NULL DEFAULT 1",
        )
        await _ensure_column(
            db,
            table="forecast_registry",
            column="is_published",
            ddl_add="ALTER TABLE forecast_registry ADD COLUMN is_published INTEGER NOT NULL DEFAULT 1",
        )
        await _ensure_column(
            db,
            table="forecast_registry",
            column="created_at",
            ddl_add="ALTER TABLE forecast_registry ADD COLUMN created_at TEXT",
        )

        await db.execute(
            """
            UPDATE forecast_registry
            SET created_at = COALESCE(created_at, updated_at, ?)
            WHERE created_at IS NULL OR created_at = ''
            """,
            (now_iso(),),
        )

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduled_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT NOT NULL,
            model_id TEXT NOT NULL,
            created_by_user_id INTEGER,
            created_by_username TEXT NOT NULL DEFAULT '',
            task_type TEXT NOT NULL DEFAULT 'auto_forecast',
            enabled INTEGER NOT NULL DEFAULT 1,
            cron_expr TEXT NOT NULL DEFAULT '',
            config_json TEXT NOT NULL DEFAULT '{}',
            last_run_at TEXT,
            next_run_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_run_id TEXT NOT NULL DEFAULT '',
            last_run_status TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            last_triggered_at TEXT,
            CHECK (task_type IN ('auto_forecast')),
            CHECK (enabled IN (0, 1))
        )
        """
    )

    if await _table_exists(db, "scheduled_tasks"):
        await _ensure_column(
            db,
            table="scheduled_tasks",
            column="created_by_user_id",
            ddl_add="ALTER TABLE scheduled_tasks ADD COLUMN created_by_user_id INTEGER",
        )
        await _ensure_column(
            db,
            table="scheduled_tasks",
            column="created_by_username",
            ddl_add="ALTER TABLE scheduled_tasks ADD COLUMN created_by_username TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="scheduled_tasks",
            column="last_run_id",
            ddl_add="ALTER TABLE scheduled_tasks ADD COLUMN last_run_id TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="scheduled_tasks",
            column="last_run_status",
            ddl_add="ALTER TABLE scheduled_tasks ADD COLUMN last_run_status TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="scheduled_tasks",
            column="last_error",
            ddl_add="ALTER TABLE scheduled_tasks ADD COLUMN last_error TEXT NOT NULL DEFAULT ''",
        )
        await _ensure_column(
            db,
            table="scheduled_tasks",
            column="last_triggered_at",
            ddl_add="ALTER TABLE scheduled_tasks ADD COLUMN last_triggered_at TEXT",
        )

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS cleanup_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL DEFAULT '',
            action TEXT NOT NULL,
            target_path TEXT NOT NULL DEFAULT '',
            detail TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        """
    )

    # Create indexes that are always safe.
    await db.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_runs_site_created
        ON runs(site_id, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_runs_user_created
        ON runs(user_id, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_runs_status
        ON runs(status);

        CREATE INDEX IF NOT EXISTS idx_runs_task_type
        ON runs(task_type);

        CREATE INDEX IF NOT EXISTS idx_runs_retention
        ON runs(retention_class);

        CREATE INDEX IF NOT EXISTS idx_runs_scheduled_task
        ON runs(scheduled_task_id, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_run_outputs_run
        ON run_outputs(run_id);

        CREATE INDEX IF NOT EXISTS idx_run_outputs_artifact
        ON run_outputs(artifact_type);

        CREATE INDEX IF NOT EXISTS idx_run_outputs_model
        ON run_outputs(model_id);

        CREATE INDEX IF NOT EXISTS idx_run_outputs_var_trt
        ON run_outputs(variable, treatment);

        CREATE INDEX IF NOT EXISTS idx_run_outputs_run_artifact_model
        ON run_outputs(run_id, artifact_type, model_id);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_lookup
        ON forecast_registry(site_id, model_id, variable, treatment, is_latest);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_run
        ON forecast_registry(source_run_id);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_updated
        ON forecast_registry(updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_pub_latest
        ON forecast_registry(site_id, is_latest, is_published, updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_site
        ON scheduled_tasks(site_id, enabled);

        CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_next
        ON scheduled_tasks(enabled, next_run_at);

        CREATE INDEX IF NOT EXISTS idx_cleanup_log_run
        ON cleanup_log(run_id);

        CREATE INDEX IF NOT EXISTS idx_cleanup_log_created
        ON cleanup_log(created_at DESC);
        """
    )

    # Create creator index only after the column is guaranteed to exist.
    if await _table_exists(db, "scheduled_tasks"):
        cur = await db.execute("PRAGMA table_info(scheduled_tasks)")
        rows = await cur.fetchall()
        cols = [r["name"] if isinstance(r, aiosqlite.Row) else r[1] for r in rows]

        if "created_by_user_id" in cols:
            await db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_creator
                ON scheduled_tasks(created_by_user_id, created_at DESC)
                """
            )


# ---------------------------------------------------------------------
# DB initialization
# ---------------------------------------------------------------------
async def init_db() -> None:
    """
    Initialize Runner DB.

    Safe to call on every startup.
    """
    db = await get_db()
    try:
        await db.executescript(DDL)
        await migrate_db(db)
        await db.commit()
    finally:
        await db.close()