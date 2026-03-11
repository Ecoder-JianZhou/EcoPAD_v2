from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field
import httpx

from app.core.db import get_db
from app.core.settings import RUNNER_SERVICE_URL
from app.core.security import now_iso
from app.api.auth import require_user

router = APIRouter()

TASK_SIMULATE = "simulate"
TASK_MCMC = "mcmc"
TASK_AUTO_FORECAST = "auto_forecast"


class SubmitIn(BaseModel):
    site: str = Field(..., description="Site ID, e.g. SPRUCE")
    name: str = Field(default="", description="Optional display name for this job")
    treatments: list[str] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)
    task: str = Field(..., description="simulate | mcmc | auto_forecast")
    parameters: dict = Field(default_factory=dict)
    da: dict = Field(default_factory=dict)
    notes: str = Field(default="", description="Optional notes for this run")


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _job_row_to_dict(row, status_override: str | None = None) -> dict:
    return {
        "id": row[0],
        "site": row[1],
        "task": row[2],
        "models": _split_csv(row[3]),
        "treatments": _split_csv(row[4]),
        "created_at": row[5],
        "status": status_override or row[6],
        "name": row[7] or "",
    }


def _normalize_site_ids_from_runner_sites_payload(data) -> list[str]:
    if not isinstance(data, dict):
        return []

    raw_sites = data.get("sites", [])
    out: list[str] = []

    for item in raw_sites:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            site_id = item.get("site_id") or item.get("id")
            if site_id:
                out.append(site_id)

    return out


async def _runner_get(path: str, params: dict | None = None, timeout: float = 10.0):
    if not RUNNER_SERVICE_URL:
        return None

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(f"{RUNNER_SERVICE_URL}{path}", params=params)
            if response.status_code != 200:
                return None
            return response.json()
    except Exception:
        return None


async def _runner_post(path: str, json_body: dict, timeout: float = 20.0):
    if not RUNNER_SERVICE_URL:
        raise HTTPException(status_code=502, detail="Runner service is not configured")

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(f"{RUNNER_SERVICE_URL}{path}", json=json_body)
    except Exception:
        raise HTTPException(status_code=502, detail="Runner unavailable")

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Runner error: {response.text}")

    return response.json()


async def _get_auto_forecast_permissions_for_user(user_id: int) -> dict[str, dict[str, bool]]:
    """
    Return per-site auto_forecast permissions only.
    Site visibility is NOT controlled here.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT site_id, can_auto_forecast
            FROM user_site_permissions
            WHERE user_id = ?
            ORDER BY site_id
            """,
            (user_id,),
        )
        rows = await cur.fetchall()
    finally:
        await db.close()

    out: dict[str, dict[str, bool]] = {}
    for row in rows:
        out[row["site_id"]] = {
            "can_auto_forecast": bool(row["can_auto_forecast"]),
        }
    return out


async def _assert_job_belongs_to_user(job_id: str, user_id: int) -> None:
    db = await get_db()
    try:
        cur = await db.execute(
            "SELECT user_id FROM jobs WHERE id = ?",
            (job_id,),
        )
        row = await cur.fetchone()
        if not row or row[0] != user_id:
            raise HTTPException(status_code=404, detail="Job not found")
    finally:
        await db.close()


@router.get("/meta")
async def workflow_meta():
    sites: list[str] = []
    models: list[str] = []
    treatments: list[str] = []

    data = await _runner_get("/api/sites", timeout=8.0)
    if data:
        sites = _normalize_site_ids_from_runner_sites_payload(data)

    first_site = sites[0] if sites else None
    if first_site:
        meta = await _runner_get(f"/api/sites/{first_site}/meta", timeout=10.0)
        if isinstance(meta, dict):
            models = meta.get("models", []) or []
            treatments = meta.get("treatments", []) or []

    return {
        "sites": sites,
        "models": models,
        "treatments": treatments,
        "tasks": [
            {"key": TASK_SIMULATE, "label": "Simulation"},
            {"key": TASK_MCMC, "label": "MCMC Assimilation"},
            {"key": TASK_AUTO_FORECAST, "label": "Auto Forecast"},
        ],
    }


@router.get("/sites")
async def wf_sites():
    data = await _runner_get("/api/sites", timeout=5.0)
    if data is None:
        return {"sites": []}
    return {"sites": _normalize_site_ids_from_runner_sites_payload(data)}


@router.get("/site_meta")
async def wf_site_meta(site: str = Query(...)):
    data = await _runner_get(f"/api/sites/{site}/meta", timeout=8.0)
    return data if data is not None else {}


@router.get("/params_meta")
async def wf_params_meta(site: str = Query(...), model: str = Query(default="")):
    params = {"model": model} if model else None
    data = await _runner_get(
        f"/api/forecast/{site}/params/meta",
        params=params,
        timeout=12.0,
    )
    return data if data is not None else None


@router.get("/permissions/me")
async def workflow_permissions_me(authorization: str | None = Header(default=None)):
    """
    Return only auto_forecast permissions.

    All users can see all sites.
    Site-level permission here only controls whether auto_forecast is allowed.
    """
    user = await require_user(authorization)

    if user["role"] == "superuser":
        data = await _runner_get("/api/sites", timeout=5.0)
        sites = _normalize_site_ids_from_runner_sites_payload(data or {})
        return {
            "role": user["role"],
            "site_permissions": {
                site_id: {
                    "can_auto_forecast": True,
                }
                for site_id in sites
            },
        }

    return {
        "role": user["role"],
        "site_permissions": await _get_auto_forecast_permissions_for_user(user["id"]),
    }


@router.post("/submit")
async def submit_job(payload: SubmitIn, authorization: str | None = Header(default=None)):
    user = await require_user(authorization)

    if payload.task == TASK_AUTO_FORECAST and user["role"] != "superuser":
        permissions = await _get_auto_forecast_permissions_for_user(user["id"])
        site_perm = permissions.get(payload.site, {})
        if not bool(site_perm.get("can_auto_forecast", False)):
            raise HTTPException(
                status_code=403,
                detail=f"You are not authorized to run auto_forecast for site {payload.site}",
            )

    result = await _runner_post(
        "/api/workflow/submit",
        json_body={
            "site": payload.site,
            "name": payload.name,
            "treatments": payload.treatments,
            "models": payload.models,
            "task": payload.task,
            "parameters": payload.parameters,
            "da": payload.da,
            "notes": payload.notes,
            "submitted_by": user["username"],
            "user_id": user["id"],
        },
        timeout=60.0,
    )

    run = result.get("run") if isinstance(result, dict) else None
    if not isinstance(run, dict):
        raise HTTPException(status_code=502, detail="Runner returned invalid run payload")

    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO jobs(id, user_id, site, task, models, treatments, created_at, status, name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run["id"],
                user["id"],
                payload.site,
                payload.task,
                ",".join(payload.models),
                ",".join(payload.treatments),
                now_iso(),
                run.get("status", "queued"),
                payload.name or payload.notes or "",
            ),
        )
        await db.commit()
    finally:
        await db.close()

    return {"ok": True, "job": run}


@router.get("/jobs")
async def list_jobs(authorization: str | None = Header(default=None)):
    user = await require_user(authorization)

    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT id, site, task, models, treatments, created_at, status, name
            FROM jobs
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user["id"],),
        )
        rows = await cur.fetchall()
    finally:
        await db.close()

    if not rows:
        return {"jobs": []}

    latest_status_by_job_id: dict[str, str] = {}

    if RUNNER_SERVICE_URL:
        async with httpx.AsyncClient(timeout=8) as client:
            for row in rows:
                job_id = row[0]
                try:
                    response = await client.get(f"{RUNNER_SERVICE_URL}/api/runs/{job_id}")
                    if response.status_code == 200:
                        payload = response.json() or {}
                        latest_status_by_job_id[job_id] = payload.get("status") or row[6]
                    else:
                        latest_status_by_job_id[job_id] = row[6]
                except Exception:
                    latest_status_by_job_id[job_id] = row[6]

    db = await get_db()
    try:
        for row in rows:
            job_id = row[0]
            old_status = row[6]
            new_status = latest_status_by_job_id.get(job_id, old_status)

            if new_status != old_status:
                await db.execute(
                    """
                    UPDATE jobs
                    SET status = ?
                    WHERE id = ? AND user_id = ?
                    """,
                    (new_status, job_id, user["id"]),
                )
        await db.commit()
    finally:
        await db.close()

    return {
        "jobs": [
            _job_row_to_dict(row, status_override=latest_status_by_job_id.get(row[0]))
            for row in rows
        ]
    }


@router.get("/jobs/{job_id}")
async def job_detail(job_id: str, authorization: str | None = Header(default=None)):
    user = await require_user(authorization)
    await _assert_job_belongs_to_user(job_id, user["id"])

    data = await _runner_get(f"/api/runs/{job_id}", timeout=15.0)
    if data is None:
        raise HTTPException(status_code=502, detail="Runner unavailable")
    return data


@router.get("/jobs/{job_id}/results")
async def job_results(
    job_id: str,
    variable: str = "GPP",
    authorization: str | None = Header(default=None),
):
    user = await require_user(authorization)
    await _assert_job_belongs_to_user(job_id, user["id"])

    data = await _runner_get(
        f"/api/workflow/runs/{job_id}/timeseries",
        params={"variable": variable},
        timeout=20.0,
    )
    if data is None:
        raise HTTPException(status_code=502, detail="Runner unavailable")
    return data


@router.get("/runs/{run_id}/manifest")
async def run_manifest(run_id: str, authorization: str | None = Header(default=None)):
    await require_user(authorization)

    data = await _runner_get(f"/api/workflow/runs/{run_id}/manifest", timeout=20.0)
    if data is None:
        raise HTTPException(status_code=502, detail="Runner unavailable")
    return data


@router.get("/runs/{run_id}/timeseries")
async def run_timeseries(
    run_id: str,
    variable: str,
    model: str = "",
    treatment: str = "",
    authorization: str | None = Header(default=None),
):
    await require_user(authorization)

    params = {"variable": variable}
    if model:
        params["model"] = model
    if treatment:
        params["treatment"] = treatment

    data = await _runner_get(
        f"/api/workflow/runs/{run_id}/timeseries",
        params=params,
        timeout=30.0,
    )
    if data is None:
        raise HTTPException(status_code=502, detail="Runner unavailable")
    return data