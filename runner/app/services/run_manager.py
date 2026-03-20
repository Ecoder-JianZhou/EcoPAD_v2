"""
Runner run management service.

Responsibilities:
- create run records
- transition run status
- query one run or list runs
- register run artifacts into run_outputs
- replace run artifacts from a site manifest
- delete terminal runs safely

This module owns platform-level run truth in Runner DB.
It does NOT execute models and does NOT parse site raw files.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from app.core.db import get_db, now_iso


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
TERMINAL_STATUSES = {"done", "failed", "cancelled"}
ACTIVE_STATUSES = {"queued", "running"}
ALL_STATUSES = TERMINAL_STATUSES | ACTIVE_STATUSES
DELETABLE_STATUSES = {"done", "failed", "cancelled"}

ALLOWED_TASK_TYPES = {
    # current canonical task types
    "simulation_without_da",
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
    "auto_forecast",
    "custom",
    # backward compatibility
    "simulate",
}

ALLOWED_TRIGGER_TYPES = {"manual", "scheduled", "system"}
ALLOWED_RETENTION_CLASSES = {"ephemeral", "normal", "published"}

ALLOWED_OUTPUT_TYPES = {
    "simulation_without_da",
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
    "auto_forecast_with_da",
    "auto_forecast_without_da",
    # backward compatibility
    "simulate",
}


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------
def _as_json_text(value: dict[str, Any] | list[Any] | None) -> str:
    """
    Convert a Python object to compact JSON text.

    Notes:
    - SQLite stores flexible structured payloads as TEXT
    - We keep compact separators to reduce DB size slightly
    """
    if value is None:
        return "{}"
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _parse_json_text(value: Any) -> Any:
    """
    Parse JSON text safely.

    Behavior:
    - None -> {}
    - dict/list -> unchanged
    - invalid JSON string -> original value
    """
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except Exception:
        return value


def _row_to_dict(row: Any) -> dict[str, Any] | None:
    """
    Convert sqlite row to plain dict.

    Extra behavior:
    - if payload_json exists, also expose parsed `payload`
    - if metadata_json exists, also expose parsed `metadata`
    - keep original *_json fields for debugging / backward compatibility
    """
    if row is None:
        return None

    obj = dict(row)

    if "payload_json" in obj:
        obj["payload"] = _parse_json_text(obj.get("payload_json"))

    if "metadata_json" in obj:
        obj["metadata"] = _parse_json_text(obj.get("metadata_json"))

    return obj


def _normalize_task_type(task_type: str) -> str:
    """
    Validate task type.
    """
    text = str(task_type or "").strip()
    if text not in ALLOWED_TASK_TYPES:
        raise ValueError(f"Unsupported task_type: {text}")
    return text


def _normalize_trigger_type(trigger_type: str) -> str:
    """
    Validate trigger type.
    """
    text = str(trigger_type or "").strip()
    if text not in ALLOWED_TRIGGER_TYPES:
        raise ValueError(f"Unsupported trigger_type: {text}")
    return text


def _normalize_retention_class(retention_class: str) -> str:
    """
    Validate retention class.
    """
    text = str(retention_class or "").strip()
    if text not in ALLOWED_RETENTION_CLASSES:
        raise ValueError(f"Unsupported retention_class: {text}")
    return text


def _normalize_status(status: str) -> str:
    """
    Validate run status.
    """
    text = str(status or "").strip()
    if text not in ALL_STATUSES:
        raise ValueError(f"Unsupported status: {text}")
    return text


def _normalize_output_type(output_type: str) -> str:
    """
    Validate artifact/output type used by forecast-facing selectors.

    Stored in metadata_json or manifest-derived catalog logic.
    """
    text = str(output_type or "").strip()
    if text not in ALLOWED_OUTPUT_TYPES:
        raise ValueError(f"Unsupported output_type: {text}")
    return text


def _csv_or_empty_list(value: list[str] | tuple[str, ...] | None) -> list[str]:
    """
    Normalize a list-like filter to a clean string list.
    """
    out: list[str] = []
    for item in value or []:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _task_default_output_type(task_type: str) -> str:
    """
    Infer default output branch from run task type.
    """
    task = str(task_type or "").strip()
    if task == "simulation_without_da" or task == "simulate":
        return "simulation_without_da"
    if task == "simulation_with_da":
        return "simulation_with_da"
    if task == "forecast_with_da":
        return "forecast_with_da"
    if task == "forecast_without_da":
        return "forecast_without_da"
    if task == "auto_forecast":
        return "auto_forecast_with_da"
    return ""


# ---------------------------------------------------------------------
# Run CRUD
# ---------------------------------------------------------------------
async def create_run(
    *,
    site_id: str,
    model_id: str,
    task_type: str,
    payload: dict[str, Any] | None = None,
    user_id: int | None = None,
    username: str = "",
    trigger_type: str = "manual",
    output_dir: str = "",
    retention_class: str = "normal",
    site_base_url: str = "",
    run_id: str | None = None,
    scheduled_task_id: int | None = None,
) -> dict[str, Any]:
    """
    Create a new run record in queued state.

    Parameters:
    - scheduled_task_id:
      Optional reference to scheduled_tasks.id.
      Important for auto-forecast chains because repeated runs can then
      be linked back to one schedule definition.
    """
    task_type = _normalize_task_type(task_type)
    trigger_type = _normalize_trigger_type(trigger_type)
    retention_class = _normalize_retention_class(retention_class)

    run_id = run_id or uuid.uuid4().hex
    now = now_iso()

    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO runs (
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, '', ?, ?, ?, '', NULL)
            """,
            (
                run_id,
                user_id,
                username or "",
                site_id,
                model_id,
                task_type,
                trigger_type,
                "queued",
                _as_json_text(payload or {}),
                output_dir or "",
                now,
                now,
                retention_class,
                site_base_url or "",
                scheduled_task_id,
            ),
        )
        await db.commit()
    finally:
        await db.close()

    run = await get_run(run_id)
    if run is None:
        raise RuntimeError(f"Failed to create run {run_id}")
    return run


async def get_run(run_id: str) -> dict[str, Any] | None:
    """
    Return one run by id.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT *
            FROM runs
            WHERE id=?
            """,
            (run_id,),
        )
        row = await cur.fetchone()
        return _row_to_dict(row)
    finally:
        await db.close()


async def list_runs(
    *,
    user_id: int | None = None,
    site_id: str | None = None,
    status: str | None = None,
    task_type: str | None = None,
    scheduled_task_id: int | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    List runs with optional filters.
    """
    where: list[str] = []
    params: list[Any] = []

    if user_id is not None:
        where.append("user_id=?")
        params.append(user_id)

    if site_id:
        where.append("site_id=?")
        params.append(site_id)

    if status:
        _normalize_status(status)
        where.append("status=?")
        params.append(status)

    if task_type:
        _normalize_task_type(task_type)
        where.append("task_type=?")
        params.append(task_type)

    if scheduled_task_id is not None:
        where.append("scheduled_task_id=?")
        params.append(int(scheduled_task_id))

    sql = "SELECT * FROM runs"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(max(1, int(limit)))

    db = await get_db()
    try:
        cur = await db.execute(sql, tuple(params))
        rows = await cur.fetchall()
        return [_row_to_dict(r) or {} for r in rows]
    finally:
        await db.close()


async def list_runs_catalog(
    *,
    site_id: str,
    models: list[str] | None = None,
    treatments: list[str] | None = None,
    variable: str = "",
    task_type: str = "",
    output_type: str = "",
    scheduled_task_id: int | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """
    Return forecast-facing run catalog rows.

    This query is intended for UI selectors such as:
    - Forecast simulations run selector
    - Forecast parameter histogram run selector
    - Future run history pickers

    Rules:
    - only runs with at least one `timeseries` artifact are included
    - one row is returned per (run, effective_model, treatment, effective_output_type)
    - if `variable` is provided, only matching series artifacts are considered
    """
    clean_models = _csv_or_empty_list(models)
    clean_treatments = _csv_or_empty_list(treatments)
    clean_variable = str(variable or "").strip()
    clean_task_type = str(task_type or "").strip()
    clean_output_type = str(output_type or "").strip()

    if clean_task_type:
        _normalize_task_type(clean_task_type)

    if clean_output_type:
        _normalize_output_type(clean_output_type)

    where: list[str] = ["r.site_id=?", "ro.artifact_type='timeseries'"]
    params: list[Any] = [site_id]

    if clean_task_type:
        where.append("r.task_type=?")
        params.append(clean_task_type)

    if scheduled_task_id is not None:
        where.append("r.scheduled_task_id=?")
        params.append(int(scheduled_task_id))

    if clean_models:
        placeholders = ",".join("?" for _ in clean_models)
        where.append(
            f"""(
                COALESCE(NULLIF(ro.model_id,''), r.model_id) IN ({placeholders})
            )"""
        )
        params.extend(clean_models)

    if clean_treatments:
        placeholders = ",".join("?" for _ in clean_treatments)
        where.append(
            f"""(
                COALESCE(NULLIF(ro.treatment,''), '') IN ({placeholders})
            )"""
        )
        params.extend(clean_treatments)

    if clean_variable:
        where.append("COALESCE(NULLIF(ro.variable,''), '')=?")
        params.append(clean_variable)

    if clean_output_type:
        where.append(
            """
            COALESCE(
                NULLIF(json_extract(ro.metadata_json, '$.output_type'), ''),
                NULLIF(json_extract(ro.metadata_json, '$.series_type'), ''),
                CASE
                    WHEN r.task_type='simulate' THEN 'simulation_without_da'
                    WHEN r.task_type='simulation_without_da' THEN 'simulation_without_da'
                    WHEN r.task_type='simulation_with_da' THEN 'simulation_with_da'
                    WHEN r.task_type='forecast_with_da' THEN 'forecast_with_da'
                    WHEN r.task_type='forecast_without_da' THEN 'forecast_without_da'
                    WHEN r.task_type='auto_forecast' THEN 'auto_forecast_with_da'
                    ELSE ''
                END
            ) = ?
            """
        )
        params.append(clean_output_type)

    sql = f"""
    SELECT
        r.id,
        r.site_id,
        r.model_id,
        r.task_type,
        r.trigger_type,
        r.status,
        r.created_at,
        r.started_at,
        r.finished_at,
        r.updated_at,
        r.scheduled_task_id,
        COALESCE(NULLIF(ro.model_id,''), r.model_id) AS catalog_model_id,
        COALESCE(NULLIF(ro.treatment,''), '') AS catalog_treatment,
        MIN(COALESCE(NULLIF(ro.variable,''), '')) AS catalog_variable,
        COALESCE(
            NULLIF(json_extract(ro.metadata_json, '$.output_type'), ''),
            NULLIF(json_extract(ro.metadata_json, '$.series_type'), ''),
            CASE
                WHEN r.task_type='simulate' THEN 'simulation_without_da'
                WHEN r.task_type='simulation_without_da' THEN 'simulation_without_da'
                WHEN r.task_type='simulation_with_da' THEN 'simulation_with_da'
                WHEN r.task_type='forecast_with_da' THEN 'forecast_with_da'
                WHEN r.task_type='forecast_without_da' THEN 'forecast_without_da'
                WHEN r.task_type='auto_forecast' THEN 'auto_forecast_with_da'
                ELSE ''
            END
        ) AS catalog_output_type
    FROM runs r
    JOIN run_outputs ro
      ON ro.run_id = r.id
    WHERE {" AND ".join(where)}
    GROUP BY
        r.id,
        r.site_id,
        r.model_id,
        r.task_type,
        r.trigger_type,
        r.status,
        r.created_at,
        r.started_at,
        r.finished_at,
        r.updated_at,
        r.scheduled_task_id,
        COALESCE(NULLIF(ro.model_id,''), r.model_id),
        COALESCE(NULLIF(ro.treatment,''), ''),
        COALESCE(
            NULLIF(json_extract(ro.metadata_json, '$.output_type'), ''),
            NULLIF(json_extract(ro.metadata_json, '$.series_type'), ''),
            CASE
                WHEN r.task_type='simulate' THEN 'simulation_without_da'
                WHEN r.task_type='simulation_without_da' THEN 'simulation_without_da'
                WHEN r.task_type='simulation_with_da' THEN 'simulation_with_da'
                WHEN r.task_type='forecast_with_da' THEN 'forecast_with_da'
                WHEN r.task_type='forecast_without_da' THEN 'forecast_without_da'
                WHEN r.task_type='auto_forecast' THEN 'auto_forecast_with_da'
                ELSE ''
            END
        )
    ORDER BY COALESCE(r.finished_at, r.updated_at, r.created_at) DESC, r.id DESC
    LIMIT ?
    """
    params.append(max(1, int(limit)))

    db = await get_db()
    try:
        cur = await db.execute(sql, tuple(params))
        rows = await cur.fetchall()
        return [_row_to_dict(r) or {} for r in rows]
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Schedule stats helpers
# ---------------------------------------------------------------------
async def count_runs_for_schedule(schedule_id: int) -> int:
    """
    Count total runs created by one scheduled task.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT COUNT(*) AS n
            FROM runs
            WHERE scheduled_task_id=?
            """,
            (int(schedule_id),),
        )
        row = await cur.fetchone()
        return int(row["n"] if row else 0)
    finally:
        await db.close()


async def get_schedule_run_stats(schedule_ids: list[int] | tuple[int, ...]) -> dict[int, dict[str, Any]]:
    """
    Return run stats for multiple scheduled task IDs.
    """
    ids = [int(x) for x in schedule_ids if x is not None]
    if not ids:
        return {}

    placeholders = ",".join("?" for _ in ids)

    db = await get_db()
    try:
        cur = await db.execute(
            f"""
            SELECT
                scheduled_task_id,
                COUNT(*) AS run_count,
                SUM(CASE WHEN status IN ('queued','running') THEN 1 ELSE 0 END) AS active_run_count
            FROM runs
            WHERE scheduled_task_id IN ({placeholders})
            GROUP BY scheduled_task_id
            """,
            tuple(ids),
        )
        rows = await cur.fetchall()

        out: dict[int, dict[str, Any]] = {
            int(i): {"run_count": 0, "active_run_count": 0}
            for i in ids
        }

        for row in rows:
            sid = int(row["scheduled_task_id"])
            out[sid] = {
                "run_count": int(row["run_count"] or 0),
                "active_run_count": int(row["active_run_count"] or 0),
            }

        return out
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Run status transitions
# ---------------------------------------------------------------------
async def set_run_status(
    run_id: str,
    *,
    status: str,
    error_message: str | None = None,
    heartbeat: bool = False,
) -> dict[str, Any]:
    """
    Generic status transition helper.
    """
    status = _normalize_status(status)
    now = now_iso()

    db = await get_db()
    try:
        existing = await db.execute(
            """
            SELECT id, status, started_at, finished_at
            FROM runs
            WHERE id=?
            """,
            (run_id,),
        )
        row = await existing.fetchone()
        if row is None:
            raise ValueError(f"Run not found: {run_id}")

        current_status = row["status"]
        started_at = row["started_at"]

        if current_status in TERMINAL_STATUSES and status in ACTIVE_STATUSES:
            raise ValueError(
                f"Cannot move terminal run {run_id} from {current_status} to {status}"
            )

        updates: list[str] = ["status=?", "updated_at=?"]
        params: list[Any] = [status, now]

        if status == "running" and not started_at:
            updates.append("started_at=?")
            params.append(now)

        if status in TERMINAL_STATUSES:
            updates.append("finished_at=?")
            params.append(now)

        if error_message is not None:
            updates.append("error_message=?")
            params.append(error_message)

        if heartbeat:
            updates.append("heartbeat_at=?")
            params.append(now)

        params.append(run_id)

        await db.execute(
            f"""
            UPDATE runs
            SET {", ".join(updates)}
            WHERE id=?
            """,
            tuple(params),
        )
        await db.commit()
    finally:
        await db.close()

    updated = await get_run(run_id)
    if updated is None:
        raise RuntimeError(f"Run disappeared after update: {run_id}")
    return updated


async def mark_run_running(run_id: str) -> dict[str, Any]:
    return await set_run_status(run_id, status="running", heartbeat=True)


async def mark_run_done(run_id: str) -> dict[str, Any]:
    return await set_run_status(run_id, status="done")


async def mark_run_failed(run_id: str, error_message: str = "") -> dict[str, Any]:
    return await set_run_status(
        run_id,
        status="failed",
        error_message=error_message or "",
    )


async def mark_run_cancelled(run_id: str, error_message: str = "") -> dict[str, Any]:
    return await set_run_status(
        run_id,
        status="cancelled",
        error_message=error_message or "",
    )


async def touch_run_heartbeat(run_id: str) -> dict[str, Any]:
    run = await get_run(run_id)
    if run is None:
        raise ValueError(f"Run not found: {run_id}")
    return await set_run_status(
        run_id,
        status=run["status"],
        heartbeat=True,
    )


# ---------------------------------------------------------------------
# Artifact registration
# ---------------------------------------------------------------------
async def add_run_output(
    *,
    run_id: str,
    artifact_type: str,
    model_id: str = "",
    variable: str = "",
    treatment: str = "",
    path: str = "",
    rel_path: str = "",
    media_type: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Register a single run artifact into run_outputs.
    """
    now = now_iso()

    db = await get_db()
    try:
        cur = await db.execute(
            """
            INSERT INTO run_outputs (
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                artifact_type,
                model_id or "",
                variable or "",
                treatment or "",
                path or "",
                rel_path or "",
                media_type or "",
                _as_json_text(metadata or {}),
                now,
            ),
        )
        output_id = cur.lastrowid
        await db.commit()

        cur2 = await db.execute(
            """
            SELECT *
            FROM run_outputs
            WHERE id=?
            """,
            (output_id,),
        )
        row = await cur2.fetchone()
        if row is None:
            raise RuntimeError("Failed to fetch inserted run_output")
        return _row_to_dict(row) or {}
    finally:
        await db.close()


async def list_run_outputs(run_id: str) -> list[dict[str, Any]]:
    """
    Return all artifacts for a run.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT *
            FROM run_outputs
            WHERE run_id=?
            ORDER BY id
            """,
            (run_id,),
        )
        rows = await cur.fetchall()
        return [_row_to_dict(r) or {} for r in rows]
    finally:
        await db.close()


async def get_first_run_output(
    run_id: str,
    artifact_type: str,
    *,
    model_id: str = "",
) -> dict[str, Any] | None:
    """
    Return the first matching artifact for a run.
    """
    db = await get_db()
    try:
        sql = """
        SELECT *
        FROM run_outputs
        WHERE run_id=?
          AND artifact_type=?
        """
        params: list[Any] = [run_id, artifact_type]

        if model_id:
            sql += " AND model_id=?"
            params.append(model_id)

        sql += " ORDER BY id LIMIT 1"

        cur = await db.execute(sql, tuple(params))
        row = await cur.fetchone()
        return _row_to_dict(row)
    finally:
        await db.close()


async def delete_run_outputs(run_id: str) -> None:
    """
    Delete all artifact rows for one run.
    """
    db = await get_db()
    try:
        await db.execute(
            """
            DELETE FROM run_outputs
            WHERE run_id=?
            """,
            (run_id,),
        )
        await db.commit()
    finally:
        await db.close()


async def replace_run_outputs_from_manifest(
    run_id: str,
    manifest: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """
    Replace all artifacts for a run using a site manifest.
    """
    manifest = manifest or {}
    artifacts = manifest.get("artifacts", []) or []

    await delete_run_outputs(run_id)

    inserted: list[dict[str, Any]] = []
    reserved_keys = {
        "artifact_type",
        "type",
        "model_id",
        "variable",
        "treatment",
        "path",
        "rel_path",
        "media_type",
    }

    for item in artifacts:
        if not isinstance(item, dict):
            continue

        artifact_type = item.get("artifact_type") or item.get("type") or "unknown"
        metadata = {k: v for k, v in item.items() if k not in reserved_keys}

        inserted_item = await add_run_output(
            run_id=run_id,
            artifact_type=str(artifact_type),
            model_id=item.get("model_id", "") or "",
            variable=item.get("variable", "") or "",
            treatment=item.get("treatment", "") or "",
            path=item.get("path", "") or "",
            rel_path=item.get("rel_path", "") or "",
            media_type=item.get("media_type", "") or "",
            metadata=metadata,
        )
        inserted.append(inserted_item)

    return inserted


# ---------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------
async def delete_run(run_id: str) -> dict[str, Any]:
    """
    Delete one terminal run safely.

    Notes:
    - DB rows only. Physical file cleanup should be handled by cleanup service
      or a dedicated purge action.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT *
            FROM runs
            WHERE id=?
            """,
            (run_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise ValueError(f"Run not found: {run_id}")

        run = dict(row)
        status = str(run.get("status") or "").lower()
        cleanup_status = str(run.get("cleanup_status") or "")

        if status not in DELETABLE_STATUSES and cleanup_status != "cleaned":
            raise ValueError(
                f"Only terminal runs can be deleted. Current status: {status or 'unknown'}"
            )

        cur2 = await db.execute(
            """
            SELECT COUNT(*) AS n
            FROM forecast_registry
            WHERE source_run_id=?
            """,
            (run_id,),
        )
        row2 = await cur2.fetchone()
        n_refs = int(row2["n"] if row2 else 0)
        if n_refs > 0:
            raise ValueError(
                "This run is referenced by forecast_registry and cannot be deleted."
            )

        await db.execute(
            """
            DELETE FROM run_outputs
            WHERE run_id=?
            """,
            (run_id,),
        )

        await db.execute(
            """
            DELETE FROM runs
            WHERE id=?
            """,
            (run_id,),
        )
        await db.commit()
        return _row_to_dict(run) or run
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Convenience query helpers
# ---------------------------------------------------------------------
async def get_run_with_outputs(run_id: str) -> dict[str, Any] | None:
    """
    Return one run plus its registered outputs.
    """
    run = await get_run(run_id)
    if run is None:
        return None

    outputs = await list_run_outputs(run_id)
    run["outputs"] = outputs
    return run


async def count_active_runs_for_site(site_id: str) -> int:
    """
    Count queued/running runs for one site.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT COUNT(*) AS n
            FROM runs
            WHERE site_id=?
              AND status IN ('queued', 'running')
            """,
            (site_id,),
        )
        row = await cur.fetchone()
        return int(row["n"] if row else 0)
    finally:
        await db.close()


async def count_active_runs_total() -> int:
    """
    Count all queued/running runs.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT COUNT(*) AS n
            FROM runs
            WHERE status IN ('queued', 'running')
            """
        )
        row = await cur.fetchone()
        return int(row["n"] if row else 0)
    finally:
        await db.close()