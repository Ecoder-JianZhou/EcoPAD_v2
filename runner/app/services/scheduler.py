"""
Runner scheduler service.

Responsibilities:
- load enabled scheduled_tasks from Runner DB
- register APScheduler jobs for auto_forecast
- create scheduled runs when triggered
- dispatch runs to Site through dispatcher
- update scheduled_tasks last_run_at / next_run_at
- expose runtime status helpers
- expose manual one-shot execution for one schedule row

Current scope:
- only supports task_type = auto_forecast
- uses cron_expr in standard crontab format
- payload is read from scheduled_tasks.config_json["payload"]

Design decisions:
- one schedule row can produce many runs over time
- every trigger creates a NEW run_id
- scheduled runs default to retention_class='normal'
- latest published forecast protection is handled by forecast_registry + cleanup,
  not by marking every scheduled run as 'published'
"""

from __future__ import annotations

import json
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.db import get_db, now_iso
from app.services.dispatcher import dispatch_run
from app.services.run_manager import create_run
from app.services.site_registry import registry


_scheduler: AsyncIOScheduler | None = None


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _safe_dict(value: Any) -> dict[str, Any]:
    """
    Convert a value to dict safely.
    """
    return value if isinstance(value, dict) else {}


def _parse_json_text(value: Any) -> dict[str, Any]:
    """
    Parse JSON text safely into dict.
    """
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}

    text = value.strip()
    if not text:
        return {}

    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _build_trigger(cron_expr: str) -> CronTrigger:
    """
    Build APScheduler cron trigger from standard crontab expression.
    """
    expr = str(cron_expr or "").strip()
    if not expr:
        raise ValueError("cron_expr is required")
    return CronTrigger.from_crontab(expr)


# ---------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------
async def _get_schedule_row(schedule_id: int) -> dict[str, Any] | None:
    """
    Load one scheduled_tasks row.
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


async def _list_enabled_schedules() -> list[dict[str, Any]]:
    """
    Load enabled scheduled_tasks rows for auto_forecast.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT *
            FROM scheduled_tasks
            WHERE enabled=1
              AND task_type='auto_forecast'
            ORDER BY id
            """
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def _update_schedule_fields(
    schedule_id: int,
    *,
    last_run_at: str | None = None,
    next_run_at: str | None = None,
    last_run_id: str | None = None,
    last_run_status: str | None = None,
    last_error: str | None = None,
    last_triggered_at: str | None = None,
) -> None:
    """
    Update selected schedule fields.
    """
    sets: list[str] = ["updated_at=?"]
    params: list[Any] = [now_iso()]

    if last_run_at is not None:
        sets.append("last_run_at=?")
        params.append(last_run_at)

    if next_run_at is not None:
        sets.append("next_run_at=?")
        params.append(next_run_at)

    if last_run_id is not None:
        sets.append("last_run_id=?")
        params.append(last_run_id)

    if last_run_status is not None:
        sets.append("last_run_status=?")
        params.append(last_run_status)

    if last_error is not None:
        sets.append("last_error=?")
        params.append(last_error)

    if last_triggered_at is not None:
        sets.append("last_triggered_at=?")
        params.append(last_triggered_at)

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


async def _refresh_next_run_at(schedule_id: int) -> None:
    """
    Refresh next_run_at from the registered APScheduler job.
    """
    if _scheduler is None:
        return

    job = _scheduler.get_job(f"scheduled-task-{schedule_id}")
    next_run_at = job.next_run_time.isoformat() if job and job.next_run_time else None
    await _update_schedule_fields(schedule_id, next_run_at=next_run_at)


# ---------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------
async def _execute_schedule_row(schedule_row: dict[str, Any]) -> dict[str, Any]:
    """
    Execute one scheduled auto_forecast row.

    Flow:
    - validate schedule row
    - validate site
    - create scheduled run
    - dispatch run
    - update schedule timestamps and status fields
    """
    schedule_id = int(schedule_row["id"])
    site_id = str(schedule_row.get("site_id") or "").strip()
    model_id = str(schedule_row.get("model_id") or "").strip()
    task_type = str(schedule_row.get("task_type") or "auto_forecast").strip()
    enabled = int(schedule_row.get("enabled") or 0)

    if enabled != 1:
        raise ValueError(f"Scheduled task is disabled: {schedule_id}")

    if task_type != "auto_forecast":
        raise ValueError(f"Unsupported scheduled task_type: {task_type}")

    if not site_id:
        raise ValueError(f"Missing site_id for schedule {schedule_id}")

    if not model_id:
        raise ValueError(f"Missing model_id for schedule {schedule_id}")

    site = registry.get_site(site_id)
    if not site:
        raise ValueError(f"Site not found in registry: {site_id}")

    if not site.get("enabled", True):
        raise ValueError(f"Site is disabled: {site_id}")

    config = _parse_json_text(schedule_row.get("config_json"))
    payload = _safe_dict(config.get("payload"))

    triggered_at = now_iso()
    await _update_schedule_fields(
        schedule_id,
        last_triggered_at=triggered_at,
        last_error="",
    )

    # IMPORTANT:
    # scheduled auto_forecast runs should NOT all be marked as published forever.
    # Only latest published forecast rows are protected through forecast_registry.
    run = await create_run(
        site_id=site_id,
        model_id=model_id,
        task_type="auto_forecast",
        payload=payload,
        user_id=None,
        username="",
        trigger_type="scheduled",
        output_dir="",
        retention_class="normal",
        site_base_url=str(site.get("base_url") or ""),
        scheduled_task_id=schedule_id,
    )

    await _update_schedule_fields(
        schedule_id,
        last_run_id=run["id"],
        last_run_status="created",
    )

    try:
        dispatch_result = await dispatch_run(run["id"])
        final_status = str(dispatch_result.get("status") or "").strip()

        await _update_schedule_fields(
            schedule_id,
            last_run_id=run["id"],
            last_run_status=final_status or "unknown",
            last_error=str(dispatch_result.get("error_message") or ""),
            last_run_at=now_iso(),
        )

        await _refresh_next_run_at(schedule_id)

        return {
            "schedule_id": schedule_id,
            "site_id": site_id,
            "model_id": model_id,
            "run_id": run["id"],
            "dispatch_result": dispatch_result,
        }

    except Exception as ex:
        await _update_schedule_fields(
            schedule_id,
            last_run_id=run["id"],
            last_run_status="failed",
            last_error=str(ex),
        )
        await _refresh_next_run_at(schedule_id)
        raise


async def _run_job(schedule_id: int) -> None:
    """
    APScheduler entrypoint for one scheduled task.
    """
    schedule_row = await _get_schedule_row(schedule_id)
    if schedule_row is None:
        return

    if int(schedule_row.get("enabled") or 0) != 1:
        return

    if str(schedule_row.get("task_type") or "") != "auto_forecast":
        return

    await _execute_schedule_row(schedule_row)


# ---------------------------------------------------------------------
# Public execution helpers
# ---------------------------------------------------------------------
async def run_scheduled_task_once(schedule_id: int) -> dict[str, Any]:
    """
    Manually execute one scheduled task immediately.
    """
    schedule_row = await _get_schedule_row(schedule_id)
    if schedule_row is None:
        raise ValueError(f"Scheduled task not found: {schedule_id}")

    return await _execute_schedule_row(schedule_row)


# ---------------------------------------------------------------------
# Registration / reload
# ---------------------------------------------------------------------
async def reload_scheduler_jobs() -> dict[str, Any]:
    """
    Reload all enabled scheduled tasks into APScheduler.
    """
    global _scheduler

    if _scheduler is None:
        raise RuntimeError("Scheduler is not initialized")

    # Remove old managed jobs
    for job in list(_scheduler.get_jobs()):
        if str(job.id).startswith("scheduled-task-"):
            _scheduler.remove_job(job.id)

    rows = await _list_enabled_schedules()
    registered: list[dict[str, Any]] = []

    for row in rows:
        schedule_id = int(row["id"])
        cron_expr = str(row.get("cron_expr") or "").strip()

        trigger = _build_trigger(cron_expr)

        job = _scheduler.add_job(
            _run_job,
            trigger=trigger,
            args=[schedule_id],
            id=f"scheduled-task-{schedule_id}",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=300,
        )

        next_run_at = job.next_run_time.isoformat() if job.next_run_time else None
        await _update_schedule_fields(schedule_id, next_run_at=next_run_at)

        registered.append(
            {
                "schedule_id": schedule_id,
                "site_id": row["site_id"],
                "model_id": row["model_id"],
                "cron_expr": cron_expr,
                "next_run_at": next_run_at,
            }
        )

    return {
        "ok": True,
        "count": len(registered),
        "registered": registered,
    }


# ---------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------
async def start_scheduler() -> dict[str, Any]:
    """
    Start APScheduler and load jobs from DB.
    """
    global _scheduler

    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone="UTC")

    if not _scheduler.running:
        _scheduler.start()

    return await reload_scheduler_jobs()


async def stop_scheduler() -> None:
    """
    Stop APScheduler.
    """
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)


def get_scheduler_status() -> dict[str, Any]:
    """
    Return lightweight scheduler runtime status.
    """
    global _scheduler

    if _scheduler is None:
        return {
            "initialized": False,
            "running": False,
            "jobs": [],
        }

    return {
        "initialized": True,
        "running": bool(_scheduler.running),
        "jobs": [
            {
                "id": job.id,
                "next_run_at": job.next_run_time.isoformat() if job.next_run_time else None,
            }
            for job in _scheduler.get_jobs()
        ],
    }