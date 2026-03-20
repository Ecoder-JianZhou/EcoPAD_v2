from __future__ import annotations

import asyncio
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException

from app.api.auth import require_user
from app.core.db import get_db
from app.core.settings import RUNNER_SERVICE_URL

router = APIRouter()


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _split_csv(value: str | None) -> list[str]:
    """
    Convert a comma-separated string from DB into a list.

    Examples:
        "TECO-SPRUCE,ELM-SPRUCE" -> ["TECO-SPRUCE", "ELM-SPRUCE"]
        "" or None -> []
    """
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _job_row_to_dict(row) -> dict[str, Any]:
    """
    Convert one DB row from the jobs table into a frontend-friendly dict.

    Expected row order:
        id, name, site, task, models, treatments, created_at, status
    """
    return {
        "id": row[0],
        "name": row[1],
        "site": row[2],
        "task": row[3],
        "models": _split_csv(row[4]),
        "treatments": _split_csv(row[5]),
        "created_at": row[6],
        "status": row[7],
        "started_at": None,
        "finished_at": None,
        "updated_at": None,
        "scheduled_task_id": None,
        "trigger_type": "",
        "error_message": "",
    }


async def _fetch_runner_run(client: httpx.AsyncClient, run_id: str) -> dict[str, Any] | None:
    """
    Fetch one run detail from Runner.

    Returns None on any failure to keep Account page resilient.
    """
    if not RUNNER_SERVICE_URL:
        return None

    try:
        resp = await client.get(f"{RUNNER_SERVICE_URL}/api/runs/{run_id}")
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _merge_job_with_runner(job: dict[str, Any], run: dict[str, Any] | None) -> dict[str, Any]:
    """
    Merge Portal-local job row with Runner run detail.
    """
    if not isinstance(run, dict):
        return job

    merged = dict(job)

    merged["status"] = run.get("status") or merged.get("status") or ""
    merged["started_at"] = run.get("started_at")
    merged["finished_at"] = run.get("finished_at")
    merged["updated_at"] = run.get("updated_at")
    merged["scheduled_task_id"] = run.get("scheduled_task_id")
    merged["trigger_type"] = run.get("trigger_type") or ""
    merged["error_message"] = run.get("error_message") or ""

    merged["site"] = merged.get("site") or run.get("site_id") or ""
    merged["task"] = merged.get("task") or run.get("task_type") or ""

    if not merged.get("models"):
        model_id = str(run.get("model_id") or "").strip()
        merged["models"] = [model_id] if model_id else []

    return merged


async def _load_jobs_from_portal_db(user_id: int) -> list[dict[str, Any]]:
    """
    Read local job rows from Portal DB.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT id, name, site, task, models, treatments, created_at, status
            FROM jobs
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,),
        )
        rows = await cur.fetchall()
        return [_job_row_to_dict(row) for row in rows]
    finally:
        await db.close()


async def _enrich_jobs_with_runner(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Enrich Portal-local jobs with Runner run details.
    """
    if not jobs or not RUNNER_SERVICE_URL:
        return jobs

    async with httpx.AsyncClient(timeout=15) as client:
        tasks = [_fetch_runner_run(client, str(job["id"])) for job in jobs]
        runner_rows = await asyncio.gather(*tasks, return_exceptions=True)

    out: list[dict[str, Any]] = []
    for job, run in zip(jobs, runner_rows):
        run_obj = None if isinstance(run, Exception) else run
        out.append(_merge_job_with_runner(job, run_obj))
    return out


async def _delete_portal_job_row(job_id: str, user_id: int) -> None:
    """
    Delete one local Portal jobs row.
    """
    db = await get_db()
    try:
        await db.execute(
            """
            DELETE FROM jobs
            WHERE id = ?
              AND user_id = ?
            """,
            (job_id, user_id),
        )
        await db.commit()
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Account / My Jobs APIs
# ---------------------------------------------------------------------
@router.get("/jobs")
async def list_my_jobs(authorization: str | None = Header(default=None)):
    """
    Return all jobs that belong to the current user.

    Behavior:
    - read base rows from Portal DB
    - enrich each row from Runner DB when available
    - keep endpoint resilient even if Runner is temporarily unavailable
    """
    user = await require_user(authorization)

    jobs = await _load_jobs_from_portal_db(int(user["id"]))
    jobs = await _enrich_jobs_with_runner(jobs)

    return {"jobs": jobs}


@router.post("/jobs/refresh")
async def refresh_my_job_status(authorization: str | None = Header(default=None)):
    """
    Refresh status for unfinished jobs of the current user by pulling data
    from Runner.

    Design notes:
    - only queued/running jobs are refreshed
    - completed jobs are left unchanged
    - failures from Runner are ignored to keep the endpoint lightweight
    - DB commit happens once at the end
    """
    user = await require_user(authorization)

    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT id
            FROM jobs
            WHERE user_id = ?
              AND (status = 'queued' OR status = 'running')
            ORDER BY created_at DESC
            """,
            (user["id"],),
        )
        rows = await cur.fetchall()
        job_ids = [row[0] for row in rows]

        if not job_ids:
            return {"ok": True, "updated": 0}

        updated_count = 0

        async with httpx.AsyncClient(timeout=15) as client:
            for job_id in job_ids:
                try:
                    response = await client.get(f"{RUNNER_SERVICE_URL}/api/runs/{job_id}")
                    if response.status_code != 200:
                        continue

                    payload = response.json() or {}
                    new_status = payload.get("status")

                    if new_status:
                        await db.execute(
                            """
                            UPDATE jobs
                            SET status = ?
                            WHERE id = ?
                            """,
                            (new_status, job_id),
                        )
                        updated_count += 1

                except Exception:
                    continue

        await db.commit()
        return {"ok": True, "updated": updated_count}

    finally:
        await db.close()


@router.delete("/jobs/{job_id}")
async def delete_my_job(
    job_id: str,
    authorization: str | None = Header(default=None),
):
    """
    Delete one job that belongs to the current user.

    Rules:
    - only the owner can delete the job
    - Portal tries to delete the corresponding Runner run first
    - if Runner run is already missing, Portal still deletes its local row
    - Portal deletes its local jobs row only after ownership check
    """
    user = await require_user(authorization)

    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT id, user_id, status
            FROM jobs
            WHERE id = ?
            """,
            (job_id,),
        )
        row = await cur.fetchone()
    finally:
        await db.close()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    row_user_id = int(row[1])
    local_status = str(row[2] or "").lower()

    if row_user_id != int(user["id"]):
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to delete this job.",
        )

    runner_run: dict[str, Any] | None = None

    if RUNNER_SERVICE_URL:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                get_resp = await client.get(f"{RUNNER_SERVICE_URL}/api/runs/{job_id}")
                if get_resp.status_code == 200:
                    payload = get_resp.json() or {}
                    runner_run = payload if isinstance(payload, dict) else None
                elif get_resp.status_code == 404:
                    runner_run = None
                else:
                    try:
                        payload = get_resp.json() or {}
                        detail = payload.get("detail") or get_resp.text
                    except Exception:
                        detail = get_resp.text or "Runner lookup failed"
                    raise HTTPException(status_code=400, detail=detail)
        except HTTPException:
            raise
        except Exception as ex:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to query runner job: {str(ex)}",
            )

    effective_status = local_status
    if isinstance(runner_run, dict):
        effective_status = str(runner_run.get("status") or local_status).lower()

    if runner_run is not None and effective_status not in {"done", "failed", "cancelled"}:
        raise HTTPException(
            status_code=400,
            detail=f"Only terminal jobs can be deleted. Current status: {effective_status or 'unknown'}",
        )

    if RUNNER_SERVICE_URL and runner_run is not None:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                delete_resp = await client.delete(f"{RUNNER_SERVICE_URL}/api/runs/{job_id}")

                if delete_resp.status_code == 404:
                    pass
                elif delete_resp.status_code != 200:
                    try:
                        payload = delete_resp.json() or {}
                        detail = payload.get("detail") or delete_resp.text
                    except Exception:
                        detail = delete_resp.text or "Runner delete failed"

                    raise HTTPException(status_code=400, detail=detail)

        except HTTPException:
            raise
        except Exception as ex:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to delete runner job: {str(ex)}",
            )

    await _delete_portal_job_row(job_id, int(user["id"]))

    return {
        "ok": True,
        "deleted_job_id": job_id,
    }