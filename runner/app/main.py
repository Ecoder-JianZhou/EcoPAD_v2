"""
EcoPAD Runner entrypoint.

Responsibilities:
- create the FastAPI application
- initialize Runner DB on startup
- load site registry on startup
- start scheduler on startup
- register Runner API routers
- provide lightweight health/debug endpoints

Runner is the control plane of EcoPAD.
It manages:
- site registry
- runs
- forecast registry
- dispatch to Site services
- scheduled auto_forecast tasks

Runner does NOT implement model logic.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.db import init_db
from app.services.site_registry import registry
from app.services.scheduler import (
    get_scheduler_status,
    start_scheduler,
    stop_scheduler,
)
from app.api import workflow, forecast, sites, runs, scheduler_ops, cleanup_ops


# ---------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------
app = FastAPI(
    title="EcoPAD Runner",
    version="0.1.0",
    description="Control plane service for EcoPAD",
)


# ---------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------
# Startup / Shutdown
# ---------------------------------------------------------------------
@app.on_event("startup")
async def startup() -> None:
    """
    Initialize Runner DB, load site registry, and start scheduler.
    """
    await init_db()
    registry.load()
    await start_scheduler()


@app.on_event("shutdown")
async def shutdown() -> None:
    """
    Stop scheduler gracefully.
    """
    await stop_scheduler()


# ---------------------------------------------------------------------
# Health endpoints
# ---------------------------------------------------------------------
@app.get("/health")
async def health():
    """
    Lightweight Runner health check.
    """
    return {
        "ok": True,
        "service": "runner",
        "sites_loaded": len(registry.list_sites(enabled_only=False)),
        "enabled_sites": len(registry.list_sites(enabled_only=True)),
        "scheduler": get_scheduler_status(),
    }


@app.get("/api/health")
async def api_health():
    """
    API-scoped health check.
    """
    return {
        "ok": True,
        "service": "runner-api",
        "scheduler": get_scheduler_status(),
    }


# ---------------------------------------------------------------------
# Lightweight debug / bootstrap endpoints
# ---------------------------------------------------------------------
@app.get("/api/sites")
async def list_sites():
    """
    Return enabled site records.

    Useful for debugging and simple admin inspection.
    """
    return {
        "sites": registry.list_sites(enabled_only=True),
    }


@app.get("/api/sites/all")
async def list_all_sites():
    """
    Return all site records, including disabled ones.
    """
    return {
        "sites": registry.list_sites(enabled_only=False),
    }


@app.get("/api/sites/{site_id}")
async def get_site(site_id: str):
    """
    Return one site record by id.
    """
    site = registry.get_site(site_id)
    if not site:
        return {
            "ok": False,
            "detail": f"Site not found: {site_id}",
        }

    return {
        "ok": True,
        "site": site,
    }


# ---------------------------------------------------------------------
# API routers
# ---------------------------------------------------------------------
app.include_router(sites.router)
app.include_router(runs.router)
app.include_router(workflow.router)
app.include_router(forecast.router)
app.include_router(scheduler_ops.router)
app.include_router(cleanup_ops.router)