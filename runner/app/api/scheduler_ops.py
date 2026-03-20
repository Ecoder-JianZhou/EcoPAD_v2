"""
Runner scheduler operations API.

Responsibilities:
- inspect scheduler runtime state
- reload scheduler jobs from DB
- list scheduled_tasks rows
- manually trigger one scheduled task once
- create / update / enable / disable / delete scheduled tasks
- list runs created by one scheduled task

This API is for scheduler operations and debugging.
"""

from __future__ import annotations

import json
from typing import Any

from apscheduler.triggers.cron import CronTrigger
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.db import get_db, now_iso
from app.services.run_manager import get_schedule_run_stats, list_runs
from app.services.scheduler import (
    get_scheduler_status,
    reload_scheduler_jobs,
    run_scheduled_task_once,
)

router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])


# ---------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------
class ScheduleCreate(BaseModel):
    site_id: str
    model_id: str
    cron_expr: str = Field(..., description="Standard crontab expression, e.g. */5 * * * *")
    enabled: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)
    created_by_user_id: int | None = None
    created_by_username: str = ""
    run_immediately: bool = True


class ScheduleUpdate(BaseModel):
    site_id: str | None = None
    model_id: str | None = None
    cron_expr: str | None = None
    enabled: int | None = None
    payload: dict[str, Any] | None = None


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _validate_cron_expr(cron_expr: str) -> str:
    """
    Validate a standard crontab expression.
    """
    expr = str(cron_expr or "").strip()
    if not expr:
        raise HTTPException(status_code=400, detail="cron_expr is required")

    try:
        CronTrigger.from_crontab(expr)
    except Exception as ex:
        raise HTTPException(status_code=400, detail=f"Invalid cron_expr: {str(ex)}")

    return expr


def _normalize_enabled(value: int | None, *, default: int = 1) -> int:
    """
    Normalize enabled flag to 0/1.
    """
    if value is None:
        return default
    return 1 if int(value) else 0


def _payload_to_config_json(payload: dict[str, Any] | None) -> str:
    """
    Convert payload dict into config_json text.
    """
    obj = {"payload": payload or {}}
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


async def _get_schedule_row(schedule_id: int) -> dict[str, Any] | None:
    """
    Return one scheduled task row.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT *
            FROM scheduled_tasks
            WHERE id=?
            """,
            (int(schedule_id),),
        )
        row = await cur.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


def _derive_task_state(row: dict[str, Any], stats: dict[str, Any] | None) -> str:
    """
    Derive a UI-friendly schedule task state.

    Rules:
    - running: there are active runs
    - enabled: task is enabled and no active runs
    - disabled: task is disabled
    """
    active_run_count = int((stats or {}).get("active_run_count") or 0)
    enabled = int(row.get("enabled") or 0)

    if active_run_count > 0:
        return "running"
    if enabled == 1:
        return "enabled"
    return "disabled"


def _merge_schedule_stats(row: dict[str, Any], stats: dict[str, Any] | None) -> dict[str, Any]:
    """
    Merge run statistics into one scheduled task row.
    """
    out = dict(row)
    s = stats or {}

    out["run_count"] = int(s.get("run_count") or 0)
    out["active_run_count"] = int(s.get("active_run_count") or 0)

    # Backward-friendly aliases for frontend use
    out["latest_run_id"] = out.get("last_run_id")
    out["latest_run_status"] = out.get("last_run_status")
    out["latest_run_at"] = out.get("last_run_at")

    # Frontend should use task_state instead of last_run_status
    out["task_state"] = _derive_task_state(out, s)

    return out


# ---------------------------------------------------------------------
# Runtime status
# ---------------------------------------------------------------------
@router.get("/status")
async def scheduler_status():
    """
    Return runtime scheduler status.
    """
    return get_scheduler_status()


# ---------------------------------------------------------------------
# Reload runtime jobs from DB
# ---------------------------------------------------------------------
@router.post("/reload")
async def scheduler_reload():
    """
    Reload scheduled jobs from scheduled_tasks table.
    """
    return await reload_scheduler_jobs()


# ---------------------------------------------------------------------
# List DB schedules
# ---------------------------------------------------------------------
@router.get("/tasks")
async def list_scheduled_tasks(
    created_by_user_id: int | None = Query(default=None),
):
    """
    Return scheduled_tasks rows, enhanced with run statistics.

    Optional filter:
    - created_by_user_id: only return tasks created by the given user
    """
    db = await get_db()
    try:
        if created_by_user_id is None:
            cur = await db.execute(
                """
                SELECT *
                FROM scheduled_tasks
                ORDER BY id DESC
                """
            )
        else:
            cur = await db.execute(
                """
                SELECT *
                FROM scheduled_tasks
                WHERE created_by_user_id = ?
                ORDER BY id DESC
                """,
                (int(created_by_user_id),),
            )

        rows = await cur.fetchall()
        tasks = [dict(r) for r in rows]
    finally:
        await db.close()

    if not tasks:
        return {"tasks": []}

    stats_map = await get_schedule_run_stats([int(t["id"]) for t in tasks])
    enriched = [_merge_schedule_stats(t, stats_map.get(int(t["id"]))) for t in tasks]

    return {"tasks": enriched}


@router.get("/tasks/{schedule_id}")
async def get_scheduled_task(schedule_id: int):
    """
    Return one scheduled task row, enhanced with run statistics.
    """
    row = await _get_schedule_row(schedule_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Scheduled task not found: {schedule_id}")

    stats_map = await get_schedule_run_stats([int(schedule_id)])
    return _merge_schedule_stats(row, stats_map.get(int(schedule_id)))


@router.get("/tasks/{schedule_id}/runs")
async def get_scheduled_task_runs(
    schedule_id: int,
    limit: int = Query(50, ge=1, le=500),
):
    """
    Return recent runs created by one scheduled task.
    """
    row = await _get_schedule_row(schedule_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Scheduled task not found: {schedule_id}")

    runs = await list_runs(
        scheduled_task_id=int(schedule_id),
        limit=int(limit),
    )

    stats_map = await get_schedule_run_stats([int(schedule_id)])

    return {
        "schedule_id": int(schedule_id),
        "task": _merge_schedule_stats(row, stats_map.get(int(schedule_id))),
        "runs": runs,
    }


# ---------------------------------------------------------------------
# Manual one-shot run
# ---------------------------------------------------------------------
@router.post("/run/{schedule_id}")
async def scheduler_run_once(schedule_id: int):
    """
    Manually execute one scheduled task immediately.
    """
    try:
        result = await run_scheduled_task_once(schedule_id)
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "run_id": result["run_id"],
            "result": result,
        }
    except ValueError as ex:
        raise HTTPException(status_code=404, detail=str(ex))
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Failed to execute schedule: {str(ex)}")


# ---------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------
@router.post("/tasks")
async def create_scheduled_task(req: ScheduleCreate):
    """
    Create one scheduled task, reload runtime jobs, and optionally run it once immediately.

    Notes:
    - run_immediately=True will create a run right away, so the new run appears
      in runs/jobs views immediately instead of waiting for the next cron tick.
    """
    cron_expr = _validate_cron_expr(req.cron_expr)
    enabled = _normalize_enabled(req.enabled, default=1)

    site_id = str(req.site_id or "").strip()
    model_id = str(req.model_id or "").strip()
    created_by_user_id = req.created_by_user_id
    created_by_username = str(req.created_by_username or "").strip()

    if not site_id:
        raise HTTPException(status_code=400, detail="site_id is required")
    if not model_id:
        raise HTTPException(status_code=400, detail="model_id is required")

    db = await get_db()
    try:
        cur = await db.execute(
            """
            INSERT INTO scheduled_tasks (
                site_id,
                model_id,
                task_type,
                enabled,
                cron_expr,
                config_json,
                last_run_at,
                next_run_at,
                created_at,
                updated_at,
                created_by_user_id,
                created_by_username
            )
            VALUES (?, ?, 'auto_forecast', ?, ?, ?, NULL, NULL, ?, ?, ?, ?)
            """,
            (
                site_id,
                model_id,
                enabled,
                cron_expr,
                _payload_to_config_json(req.payload),
                now_iso(),
                now_iso(),
                created_by_user_id,
                created_by_username,
            ),
        )
        schedule_id = int(cur.lastrowid)
        await db.commit()
    finally:
        await db.close()

    await reload_scheduler_jobs()

    immediate_run: dict[str, Any] | None = None
    immediate_run_error = ""

    if req.run_immediately and enabled == 1:
        try:
            immediate_run = await run_scheduled_task_once(schedule_id)
        except Exception as ex:
            immediate_run_error = str(ex)

    row = await _get_schedule_row(schedule_id)
    stats_map = await get_schedule_run_stats([schedule_id])

    return {
        "ok": True,
        "task": _merge_schedule_stats(row or {}, stats_map.get(schedule_id)),
        "immediate_run": immediate_run,
        "immediate_run_error": immediate_run_error,
    }


# ---------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------
@router.patch("/tasks/{schedule_id}")
async def update_scheduled_task(schedule_id: int, req: ScheduleUpdate):
    """
    Update one scheduled task and reload runtime jobs.
    """
    existing = await _get_schedule_row(schedule_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Scheduled task not found: {schedule_id}")

    sets: list[str] = []
    params: list[Any] = []

    if req.site_id is not None:
        site_id = str(req.site_id or "").strip()
        if not site_id:
            raise HTTPException(status_code=400, detail="site_id cannot be empty")
        sets.append("site_id=?")
        params.append(site_id)

    if req.model_id is not None:
        model_id = str(req.model_id or "").strip()
        if not model_id:
            raise HTTPException(status_code=400, detail="model_id cannot be empty")
        sets.append("model_id=?")
        params.append(model_id)

    if req.cron_expr is not None:
        cron_expr = _validate_cron_expr(req.cron_expr)
        sets.append("cron_expr=?")
        params.append(cron_expr)

    if req.enabled is not None:
        sets.append("enabled=?")
        params.append(_normalize_enabled(req.enabled, default=1))

    if req.payload is not None:
        sets.append("config_json=?")
        params.append(_payload_to_config_json(req.payload))

    sets.append("updated_at=?")
    params.append(now_iso())

    if len(sets) == 1:
        row = await _get_schedule_row(schedule_id)
        stats_map = await get_schedule_run_stats([schedule_id])
        return {
            "ok": True,
            "task": _merge_schedule_stats(row or {}, stats_map.get(schedule_id)),
        }

    params.append(int(schedule_id))

    db = await get_db()
    try:
        await db.execute(
            f"""
            UPDATE scheduled_tasks
            SET {", ".join(sets)}
            WHERE id=?
            """,
            tuple(params),
        )
        await db.commit()
    finally:
        await db.close()

    await reload_scheduler_jobs()

    row = await _get_schedule_row(schedule_id)
    stats_map = await get_schedule_run_stats([schedule_id])

    return {
        "ok": True,
        "task": _merge_schedule_stats(row or {}, stats_map.get(schedule_id)),
    }


# ---------------------------------------------------------------------
# Enable / Disable
# ---------------------------------------------------------------------
@router.post("/tasks/{schedule_id}/enable")
async def enable_scheduled_task(schedule_id: int):
    """
    Enable one scheduled task and reload runtime jobs.
    """
    row = await _get_schedule_row(schedule_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Scheduled task not found: {schedule_id}")

    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE scheduled_tasks
            SET enabled=1,
                updated_at=?
            WHERE id=?
            """,
            (now_iso(), int(schedule_id)),
        )
        await db.commit()
    finally:
        await db.close()

    await reload_scheduler_jobs()

    row = await _get_schedule_row(schedule_id)
    stats_map = await get_schedule_run_stats([schedule_id])

    return {
        "ok": True,
        "task": _merge_schedule_stats(row or {}, stats_map.get(schedule_id)),
    }


@router.post("/tasks/{schedule_id}/disable")
async def disable_scheduled_task(schedule_id: int):
    """
    Disable one scheduled task and reload runtime jobs.
    """
    row = await _get_schedule_row(schedule_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Scheduled task not found: {schedule_id}")

    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE scheduled_tasks
            SET enabled=0,
                updated_at=?
            WHERE id=?
            """,
            (now_iso(), int(schedule_id)),
        )
        await db.commit()
    finally:
        await db.close()

    await reload_scheduler_jobs()

    row = await _get_schedule_row(schedule_id)
    stats_map = await get_schedule_run_stats([schedule_id])

    return {
        "ok": True,
        "task": _merge_schedule_stats(row or {}, stats_map.get(schedule_id)),
    }


# ---------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------
@router.delete("/tasks/{schedule_id}")
async def delete_scheduled_task(schedule_id: int):
    """
    Delete one scheduled task and reload runtime jobs.
    """
    row = await _get_schedule_row(schedule_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Scheduled task not found: {schedule_id}")

    db = await get_db()
    try:
        await db.execute(
            """
            DELETE FROM scheduled_tasks
            WHERE id=?
            """,
            (int(schedule_id),),
        )
        await db.commit()
    finally:
        await db.close()

    await reload_scheduler_jobs()

    return {
        "ok": True,
        "deleted_id": int(schedule_id),
    }