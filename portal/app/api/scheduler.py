from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Body, Header, HTTPException, Query

from app.api.auth import require_user
from app.core.db import get_db
from app.core.settings import RUNNER_SERVICE_URL

router = APIRouter()


# ---------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------
async def _require_superuser(authorization: str | None) -> dict[str, Any]:
    """
    Only superuser can perform platform-level scheduler management actions.
    """
    user = await require_user(authorization)
    if str(user.get("role") or "") != "superuser":
        raise HTTPException(status_code=403, detail="Superuser access required")
    return user


async def _require_scheduler_reader(authorization: str | None) -> dict[str, Any]:
    """
    Any authenticated user can access the Schedules page.
    The actual task-level visibility is filtered later.
    """
    return await require_user(authorization)


async def _list_user_allowed_sites(user_id: int) -> list[str]:
    """
    Return all site IDs where the user has auto-forecast permission.

    This permission is used for:
    - creating schedules for a site
    - deciding whether a non-superuser may create new schedule tasks there
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT site_id
            FROM user_site_permissions
            WHERE user_id = ?
              AND can_auto_forecast = 1
            ORDER BY site_id
            """,
            (int(user_id),),
        )
        rows = await cur.fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
    finally:
        await db.close()


async def _check_site_create_permission(user: dict[str, Any], site_id: str) -> None:
    """
    Check whether the current user may create a schedule for a given site.

    Rules:
    - superuser: always allowed
    - normal user: allowed only if site permission exists
    """
    if str(user.get("role") or "") == "superuser":
        return

    user_id = int(user["id"])
    allowed_sites = await _list_user_allowed_sites(user_id)
    if str(site_id or "") not in allowed_sites:
        raise HTTPException(
            status_code=403,
            detail=f"You do not have auto-forecast permission for site: {site_id}",
        )


def _filter_tasks_for_user(user: dict[str, Any], tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Filter schedule tasks for one user.

    Rules:
    - superuser: see all tasks
    - normal user: see only tasks created by themselves
    """
    if str(user.get("role") or "") == "superuser":
        return tasks

    my_user_id = int(user["id"])
    out: list[dict[str, Any]] = []

    for task in tasks:
        owner_id = task.get("created_by_user_id")
        if owner_id is None:
            continue
        try:
            if int(owner_id) == my_user_id:
                out.append(task)
        except Exception:
            continue

    return out


def _user_can_access_task(user: dict[str, Any], task: dict[str, Any] | None) -> bool:
    """
    Check whether a user may view/manage one schedule task.

    Rules:
    - superuser: yes
    - normal user: only if created_by_user_id matches
    """
    if not task:
        return False

    if str(user.get("role") or "") == "superuser":
        return True

    try:
        return int(task.get("created_by_user_id")) == int(user["id"])
    except Exception:
        return False


# ---------------------------------------------------------------------
# Runner proxy helpers
# ---------------------------------------------------------------------
async def _runner_request(
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    query_params: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """
    Proxy one request to Runner and preserve status/error details.
    """
    if not RUNNER_SERVICE_URL:
        raise HTTPException(status_code=500, detail="RUNNER_SERVICE_URL is not configured")

    url = f"{RUNNER_SERVICE_URL}{path}"

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.request(
                method,
                url,
                json=json_body,
                params=query_params,
            )

        try:
            data = resp.json()
        except Exception:
            data = {"detail": resp.text or resp.reason_phrase or "Runner request failed"}

        if resp.status_code >= 400:
            raise HTTPException(
                status_code=resp.status_code,
                detail=data.get("detail") if isinstance(data, dict) else str(data),
            )

        if isinstance(data, dict):
            return data

        return {"ok": True, "data": data}

    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=502, detail=f"Runner request failed: {ex}")


async def _runner_get(
    path: str,
    *,
    query_params: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    return await _runner_request("GET", path, query_params=query_params, timeout=timeout)


async def _runner_post(
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    query_params: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    return await _runner_request(
        "POST",
        path,
        json_body=json_body,
        query_params=query_params,
        timeout=timeout,
    )


async def _runner_patch(
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    return await _runner_request(
        "PATCH",
        path,
        json_body=json_body,
        timeout=timeout,
    )


async def _runner_delete(
    path: str,
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    return await _runner_request("DELETE", path, timeout=timeout)


# ---------------------------------------------------------------------
# Runtime status
# ---------------------------------------------------------------------
@router.get("/status")
async def scheduler_status(authorization: str | None = Header(default=None)):
    """
    Return scheduler runtime status from Runner.

    This is platform-level runtime info, so it remains superuser-only.
    """
    await _require_superuser(authorization)
    return await _runner_get("/api/scheduler/status", timeout=8.0)


# ---------------------------------------------------------------------
# Reload runtime jobs
# ---------------------------------------------------------------------
@router.post("/reload")
async def scheduler_reload(authorization: str | None = Header(default=None)):
    """
    Reload scheduler runtime jobs from Runner DB.

    This is platform-level control, so it remains superuser-only.
    """
    await _require_superuser(authorization)
    return await _runner_post("/api/scheduler/reload", timeout=15.0)


# ---------------------------------------------------------------------
# List / get scheduled tasks
# ---------------------------------------------------------------------
@router.get("/tasks")
async def list_scheduled_tasks(authorization: str | None = Header(default=None)):
    """
    Return schedule tasks.

    Rules:
    - superuser: see all tasks
    - normal user: see only tasks created by themselves
    """
    user = await _require_scheduler_reader(authorization)
    data = await _runner_get("/api/scheduler/tasks", timeout=10.0)
    tasks = data.get("tasks") or []
    return {"tasks": _filter_tasks_for_user(user, tasks)}


@router.get("/tasks/{schedule_id}")
async def get_scheduled_task(
    schedule_id: int,
    authorization: str | None = Header(default=None),
):
    """
    Return one schedule task if the user has access.
    """
    user = await _require_scheduler_reader(authorization)
    task = await _runner_get(f"/api/scheduler/tasks/{schedule_id}", timeout=10.0)

    if not _user_can_access_task(user, task):
        raise HTTPException(status_code=403, detail="You do not have access to this schedule")

    return task


@router.get("/tasks/{schedule_id}/runs")
async def get_scheduled_task_runs(
    schedule_id: int,
    authorization: str | None = Header(default=None),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Return recent runs for one schedule task.

    Rules:
    - superuser: can access any task
    - normal user: only their own task
    """
    user = await _require_scheduler_reader(authorization)
    task = await _runner_get(f"/api/scheduler/tasks/{schedule_id}", timeout=10.0)

    if not _user_can_access_task(user, task):
        raise HTTPException(status_code=403, detail="You do not have access to this schedule")

    return await _runner_get(
        f"/api/scheduler/tasks/{schedule_id}/runs",
        query_params={"limit": limit},
        timeout=15.0,
    )


# ---------------------------------------------------------------------
# Run once
# ---------------------------------------------------------------------
@router.post("/run/{schedule_id}")
async def scheduler_run_once(
    schedule_id: int,
    authorization: str | None = Header(default=None),
):
    """
    Manually execute one scheduled task immediately.

    Rules:
    - superuser: can run any task
    - normal user: can run only their own task
    """
    user = await _require_scheduler_reader(authorization)
    task = await _runner_get(f"/api/scheduler/tasks/{schedule_id}", timeout=10.0)

    if not _user_can_access_task(user, task):
        raise HTTPException(status_code=403, detail="You do not have access to this schedule")

    return await _runner_post(f"/api/scheduler/run/{schedule_id}", timeout=60.0)


# ---------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------
@router.post("/tasks")
async def create_scheduled_task(
    payload: dict[str, Any] = Body(...),
    authorization: str | None = Header(default=None),
):
    """
    Create one scheduled task.

    Rules:
    - superuser: may create for any site
    - normal user: may create only for sites where can_auto_forecast = 1
    """
    user = await require_user(authorization)

    site_id = str(payload.get("site_id") or "").strip()
    if not site_id:
        raise HTTPException(status_code=400, detail="site_id is required")

    await _check_site_create_permission(user, site_id)

    body = dict(payload or {})
    body["created_by_user_id"] = int(user["id"])
    body["created_by_username"] = str(user.get("username") or "")

    return await _runner_post(
        "/api/scheduler/tasks",
        json_body=body,
        timeout=15.0,
    )


# ---------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------
@router.patch("/tasks/{schedule_id}")
async def update_scheduled_task(
    schedule_id: int,
    payload: dict[str, Any] = Body(...),
    authorization: str | None = Header(default=None),
):
    """
    Update one scheduled task.

    Rules:
    - superuser: can update any task
    - normal user: can update only their own task
    """
    user = await _require_scheduler_reader(authorization)
    task = await _runner_get(f"/api/scheduler/tasks/{schedule_id}", timeout=10.0)

    if not _user_can_access_task(user, task):
        raise HTTPException(status_code=403, detail="You do not have access to this schedule")

    return await _runner_patch(
        f"/api/scheduler/tasks/{schedule_id}",
        json_body=payload,
        timeout=15.0,
    )


# ---------------------------------------------------------------------
# Enable / disable
# ---------------------------------------------------------------------
@router.post("/tasks/{schedule_id}/enable")
async def enable_scheduled_task(
    schedule_id: int,
    authorization: str | None = Header(default=None),
):
    """
    Enable one scheduled task.

    Rules:
    - superuser: can enable any task
    - normal user: can enable only their own task
    """
    user = await _require_scheduler_reader(authorization)
    task = await _runner_get(f"/api/scheduler/tasks/{schedule_id}", timeout=10.0)

    if not _user_can_access_task(user, task):
        raise HTTPException(status_code=403, detail="You do not have access to this schedule")

    return await _runner_post(
        f"/api/scheduler/tasks/{schedule_id}/enable",
        timeout=15.0,
    )


@router.post("/tasks/{schedule_id}/disable")
async def disable_scheduled_task(
    schedule_id: int,
    authorization: str | None = Header(default=None),
):
    """
    Disable one scheduled task.

    Rules:
    - superuser: can disable any task
    - normal user: can disable only their own task
    """
    user = await _require_scheduler_reader(authorization)
    task = await _runner_get(f"/api/scheduler/tasks/{schedule_id}", timeout=10.0)

    if not _user_can_access_task(user, task):
        raise HTTPException(status_code=403, detail="You do not have access to this schedule")

    return await _runner_post(
        f"/api/scheduler/tasks/{schedule_id}/disable",
        timeout=15.0,
    )


# ---------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------
@router.delete("/tasks/{schedule_id}")
async def delete_scheduled_task(
    schedule_id: int,
    authorization: str | None = Header(default=None),
):
    """
    Delete one scheduled task.

    Rules:
    - superuser: can delete any task
    - normal user: can delete only their own task
    """
    user = await _require_scheduler_reader(authorization)
    task = await _runner_get(f"/api/scheduler/tasks/{schedule_id}", timeout=10.0)

    if not _user_can_access_task(user, task):
        raise HTTPException(status_code=403, detail="You do not have access to this schedule")

    return await _runner_delete(
        f"/api/scheduler/tasks/{schedule_id}",
        timeout=15.0,
    )