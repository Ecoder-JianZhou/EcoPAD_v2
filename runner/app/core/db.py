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

    CHECK (
        task_type IN (
            'simulation_without_da',
            'simulation_with_da',
            'forecast_with_da',
            'forecast_without_da',
            'auto_forecast',
            'custom'
        )
    ),
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
-- Published/latest series index for Forecast page.
-- =========================================================
CREATE TABLE IF NOT EXISTS forecast_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    variable TEXT NOT NULL,
    treatment TEXT NOT NULL,
    series_type TEXT NOT NULL DEFAULT 'forecast_with_da',
    source_run_id TEXT NOT NULL,
    data_path TEXT NOT NULL DEFAULT '',
    obs_path TEXT NOT NULL DEFAULT '',
    source_ref_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL,
    is_latest INTEGER NOT NULL DEFAULT 1,
    is_published INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CHECK (
        series_type IN (
            'simulation_without_da',
            'simulation_with_da',
            'forecast_with_da',
            'forecast_without_da'
        )
    ),
    CHECK (is_latest IN (0, 1)),
    CHECK (is_published IN (0, 1)),

    FOREIGN KEY(source_run_id) REFERENCES runs(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_lookup
ON forecast_registry(site_id, model_id, variable, treatment, is_latest);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_lookup_type
ON forecast_registry(site_id, model_id, variable, treatment, series_type, is_latest);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_run
ON forecast_registry(source_run_id);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_run_type
ON forecast_registry(source_run_id, series_type);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_updated
ON forecast_registry(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_pub_latest
ON forecast_registry(site_id, is_latest, is_published, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_forecast_registry_pub_latest_type
ON forecast_registry(site_id, series_type, is_latest, is_published, updated_at DESC);


-- =========================================================
-- scheduled_tasks
-- Recurring auto forecast plans.
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


async def _get_table_sql(db: aiosqlite.Connection, table: str) -> str:
    """
    Return the CREATE TABLE SQL stored in sqlite_master.
    """
    cur = await db.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table,),
    )
    row = await cur.fetchone()
    if row is None:
        return ""
    return str(row["sql"] if isinstance(row, aiosqlite.Row) else row[0] or "")


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


async def _needs_legacy_task_type_rebuild(db: aiosqlite.Connection) -> bool:
    """
    Return True if the runs table still uses the old legacy task_type constraint.
    """
    if not await _table_exists(db, "runs"):
        return False

    sql = (await _get_table_sql(db, "runs")).lower()
    if not sql:
        return False

    return "'simulate'" in sql and "'simulation_without_da'" not in sql


async def _needs_legacy_series_type_rebuild(db: aiosqlite.Connection) -> bool:
    """
    Return True if forecast_registry still uses the old legacy simulate series_type.
    """
    if not await _table_exists(db, "forecast_registry"):
        return False

    sql = (await _get_table_sql(db, "forecast_registry")).lower()
    if not sql:
        return False

    return "'simulate'" in sql and "'simulation_without_da'" not in sql


async def _rebuild_runs_family_tables(db: aiosqlite.Connection) -> None:
    """
    Rebuild runs, run_outputs, and forecast_registry with the canonical schema.

    Why this rebuild is needed:
    - SQLite cannot alter CHECK constraints in place.
    - Older runner.db files may still restrict runs.task_type to 'simulate'.
    - Older forecast_registry files may still restrict series_type to 'simulate'.

    Migration behavior:
    - maps old task_type 'simulate' -> 'simulation_without_da'
    - maps old series_type 'simulate' -> 'simulation_without_da'
    - preserves all existing data
    """
    await db.execute("PRAGMA foreign_keys = OFF;")

    # -----------------------------------------------------------------
    # runs_new
    # -----------------------------------------------------------------
    await db.execute(
        """
        CREATE TABLE runs_new (
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

            CHECK (
                task_type IN (
                    'simulation_without_da',
                    'simulation_with_da',
                    'forecast_with_da',
                    'forecast_without_da',
                    'auto_forecast',
                    'custom'
                )
            ),
            CHECK (trigger_type IN ('manual', 'scheduled', 'system')),
            CHECK (status IN ('queued', 'running', 'done', 'failed', 'cancelled')),
            CHECK (retention_class IN ('ephemeral', 'normal', 'published'))
        )
        """
    )

    if await _table_exists(db, "runs"):
        await db.execute(
            """
            INSERT INTO runs_new (
                id,
                user_id,
                username,
                site_id,
                model_id,
                task_type,
                trigger_type,
                status,
                payload_json,
                output_dir,
                created_at,
                started_at,
                finished_at,
                updated_at,
                heartbeat_at,
                error_message,
                retention_class,
                site_base_url,
                scheduled_task_id,
                cleanup_status,
                cleaned_at
            )
            SELECT
                id,
                user_id,
                COALESCE(username, ''),
                site_id,
                model_id,
                CASE
                    WHEN task_type='simulate' THEN 'simulation_without_da'
                    ELSE task_type
                END AS task_type,
                COALESCE(trigger_type, 'manual'),
                COALESCE(status, 'queued'),
                COALESCE(payload_json, '{}'),
                COALESCE(output_dir, ''),
                created_at,
                started_at,
                finished_at,
                updated_at,
                heartbeat_at,
                COALESCE(error_message, ''),
                COALESCE(retention_class, 'normal'),
                COALESCE(site_base_url, ''),
                scheduled_task_id,
                COALESCE(cleanup_status, ''),
                cleaned_at
            FROM runs
            """
        )

    # -----------------------------------------------------------------
    # run_outputs_new
    # -----------------------------------------------------------------
    await db.execute(
        """
        CREATE TABLE run_outputs_new (
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

            FOREIGN KEY(run_id) REFERENCES runs_new(id) ON DELETE CASCADE
        )
        """
    )

    if await _table_exists(db, "run_outputs"):
        await db.execute(
            """
            INSERT INTO run_outputs_new (
                id,
                run_id,
                artifact_type,
                model_id,
                variable,
                treatment,
                path,
                rel_path,
                media_type,
                metadata_json,
                created_at
            )
            SELECT
                id,
                run_id,
                artifact_type,
                COALESCE(model_id, ''),
                COALESCE(variable, ''),
                COALESCE(treatment, ''),
                COALESCE(path, ''),
                COALESCE(rel_path, ''),
                COALESCE(media_type, ''),
                COALESCE(metadata_json, '{}'),
                created_at
            FROM run_outputs
            """
        )

    # -----------------------------------------------------------------
    # forecast_registry_new
    # -----------------------------------------------------------------
    await db.execute(
        """
        CREATE TABLE forecast_registry_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT NOT NULL,
            model_id TEXT NOT NULL,
            variable TEXT NOT NULL,
            treatment TEXT NOT NULL,
            series_type TEXT NOT NULL DEFAULT 'forecast_with_da',
            source_run_id TEXT NOT NULL,
            data_path TEXT NOT NULL DEFAULT '',
            obs_path TEXT NOT NULL DEFAULT '',
            source_ref_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            is_latest INTEGER NOT NULL DEFAULT 1,
            is_published INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

            CHECK (
                series_type IN (
                    'simulation_without_da',
                    'simulation_with_da',
                    'forecast_with_da',
                    'forecast_without_da'
                )
            ),
            CHECK (is_latest IN (0, 1)),
            CHECK (is_published IN (0, 1)),

            FOREIGN KEY(source_run_id) REFERENCES runs_new(id) ON DELETE RESTRICT
        )
        """
    )

    if await _table_exists(db, "forecast_registry"):
        cur = await db.execute("PRAGMA table_info(forecast_registry)")
        rows = await cur.fetchall()
        cols = {r["name"] if isinstance(r, aiosqlite.Row) else r[1] for r in rows}

        has_series_type = "series_type" in cols
        has_forecast_mode = "forecast_mode" in cols

        if has_series_type:
            await db.execute(
                """
                INSERT INTO forecast_registry_new (
                    id,
                    site_id,
                    model_id,
                    variable,
                    treatment,
                    series_type,
                    source_run_id,
                    data_path,
                    obs_path,
                    source_ref_json,
                    updated_at,
                    is_latest,
                    is_published,
                    created_at
                )
                SELECT
                    id,
                    site_id,
                    model_id,
                    variable,
                    treatment,
                    CASE
                        WHEN series_type='simulate' THEN 'simulation_without_da'
                        WHEN series_type IS NULL OR series_type='' THEN 'forecast_with_da'
                        ELSE series_type
                    END AS series_type,
                    source_run_id,
                    COALESCE(data_path, ''),
                    COALESCE(obs_path, ''),
                    COALESCE(source_ref_json, '{}'),
                    updated_at,
                    COALESCE(is_latest, 1),
                    COALESCE(is_published, 1),
                    COALESCE(created_at, updated_at, ?)
                FROM forecast_registry
                """,
                (now_iso(),),
            )
        elif has_forecast_mode:
            await db.execute(
                """
                INSERT INTO forecast_registry_new (
                    id,
                    site_id,
                    model_id,
                    variable,
                    treatment,
                    series_type,
                    source_run_id,
                    data_path,
                    obs_path,
                    source_ref_json,
                    updated_at,
                    is_latest,
                    is_published,
                    created_at
                )
                SELECT
                    id,
                    site_id,
                    model_id,
                    variable,
                    treatment,
                    CASE
                        WHEN forecast_mode='simulate' THEN 'simulation_without_da'
                        WHEN forecast_mode IS NULL OR forecast_mode='' THEN 'forecast_with_da'
                        ELSE forecast_mode
                    END AS series_type,
                    source_run_id,
                    COALESCE(data_path, ''),
                    COALESCE(obs_path, ''),
                    COALESCE(source_ref_json, '{}'),
                    updated_at,
                    COALESCE(is_latest, 1),
                    COALESCE(is_published, 1),
                    COALESCE(created_at, updated_at, ?)
                FROM forecast_registry
                """,
                (now_iso(),),
            )
        else:
            await db.execute(
                """
                INSERT INTO forecast_registry_new (
                    id,
                    site_id,
                    model_id,
                    variable,
                    treatment,
                    series_type,
                    source_run_id,
                    data_path,
                    obs_path,
                    source_ref_json,
                    updated_at,
                    is_latest,
                    is_published,
                    created_at
                )
                SELECT
                    id,
                    site_id,
                    model_id,
                    variable,
                    treatment,
                    'forecast_with_da',
                    source_run_id,
                    '',
                    '',
                    '{}',
                    updated_at,
                    1,
                    1,
                    COALESCE(updated_at, ?)
                FROM forecast_registry
                """,
                (now_iso(),),
            )

    # -----------------------------------------------------------------
    # Swap tables
    # -----------------------------------------------------------------
    if await _table_exists(db, "forecast_registry"):
        await db.execute("DROP TABLE forecast_registry")
    if await _table_exists(db, "run_outputs"):
        await db.execute("DROP TABLE run_outputs")
    if await _table_exists(db, "runs"):
        await db.execute("DROP TABLE runs")

    await db.execute("ALTER TABLE runs_new RENAME TO runs")
    await db.execute("ALTER TABLE run_outputs_new RENAME TO run_outputs")
    await db.execute("ALTER TABLE forecast_registry_new RENAME TO forecast_registry")

    await db.execute("PRAGMA foreign_keys = ON;")


async def migrate_db(db: aiosqlite.Connection) -> None:
    """
    Apply lightweight migrations for older runner.db files.

    Notes:
    - Additive migrations are handled with ALTER TABLE where possible.
    - Legacy CHECK constraints require table rebuilds.
    - Safe to call on every startup.
    """
    # -----------------------------------------------------------------
    # Additive migrations first
    # -----------------------------------------------------------------
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
        await _ensure_column(
            db,
            table="forecast_registry",
            column="series_type",
            ddl_add="ALTER TABLE forecast_registry ADD COLUMN series_type TEXT NOT NULL DEFAULT 'forecast_with_da'",
        )

        cur = await db.execute("PRAGMA table_info(forecast_registry)")
        rows = await cur.fetchall()
        cols = [r["name"] if isinstance(r, aiosqlite.Row) else r[1] for r in rows]

        if "forecast_mode" in cols:
            await db.execute(
                """
                UPDATE forecast_registry
                SET series_type = COALESCE(NULLIF(series_type, ''), forecast_mode, 'forecast_with_da')
                WHERE series_type IS NULL OR series_type = ''
                """
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
            UPDATE forecast_registry
            SET series_type = COALESCE(NULLIF(series_type, ''), 'forecast_with_da')
            WHERE series_type IS NULL OR series_type = ''
            """
        )

        await db.execute(
            """
            UPDATE forecast_registry
            SET series_type = 'simulation_without_da'
            WHERE series_type = 'simulate'
            """
        )

    # -----------------------------------------------------------------
    # Rebuild legacy constrained tables when needed
    # -----------------------------------------------------------------
    if await _needs_legacy_task_type_rebuild(db) or await _needs_legacy_series_type_rebuild(db):
        await _rebuild_runs_family_tables(db)

    # -----------------------------------------------------------------
    # scheduled_tasks
    # -----------------------------------------------------------------
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

    # -----------------------------------------------------------------
    # cleanup_log
    # -----------------------------------------------------------------
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

    # -----------------------------------------------------------------
    # Indexes
    # -----------------------------------------------------------------
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

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_lookup_type
        ON forecast_registry(site_id, model_id, variable, treatment, series_type, is_latest);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_run
        ON forecast_registry(source_run_id);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_run_type
        ON forecast_registry(source_run_id, series_type);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_updated
        ON forecast_registry(updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_pub_latest
        ON forecast_registry(site_id, is_latest, is_published, updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_forecast_registry_pub_latest_type
        ON forecast_registry(site_id, series_type, is_latest, is_published, updated_at DESC);

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