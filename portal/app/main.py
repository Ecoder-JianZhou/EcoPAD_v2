"""
EcoPAD Portal entrypoint.

Responsibilities:
- create the FastAPI application
- initialize Portal DB on startup
- register Portal API routers
- serve static frontend files
- serve the SPA index page

Portal is the user-facing layer only.
It should not contain Runner orchestration logic or site execution logic.
"""


from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.core.db import init_db
from app.api import auth, workflow, forecast, account, admin, setup, scheduler, cleanup, runs


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
# Actual project structure:
# portal/
#   app/
#     main.py
#     static/
#       index.html
#       app.js
#       style.css
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
INDEX_FILE = STATIC_DIR / "index.html"


# ---------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------
app = FastAPI(
    title="EcoPAD Portal",
    version="0.1.0",
    description="User-facing portal for EcoPAD",
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
# Startup
# ---------------------------------------------------------------------
@app.on_event("startup")
async def startup() -> None:
    """
    Initialize Portal DB when the application starts.
    """
    await init_db()


# ---------------------------------------------------------------------
# Health / debug endpoints
# ---------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True, "service": "portal"}


@app.get("/api/health")
async def api_health():
    return {"ok": True, "service": "portal-api"}


# ---------------------------------------------------------------------
# API routers
# ---------------------------------------------------------------------
app.include_router(setup.router, prefix="/api/setup", tags=["setup"])
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])
app.include_router(workflow.router, prefix="/api/workflow", tags=["workflow"])
app.include_router(forecast.router, prefix="/api/forecast", tags=["forecast"])
app.include_router(account.router, prefix="/api/account", tags=["account"])
app.include_router(scheduler.router, prefix="/api/scheduler", tags=["scheduler"])
app.include_router(cleanup.router, prefix="/api/cleanup", tags=["cleanup"])
app.include_router(runs.router, prefix="/api/runs", tags=["runs"])

# ---------------------------------------------------------------------
# Static files
# ---------------------------------------------------------------------
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------
# SPA entry
# ---------------------------------------------------------------------
@app.get("/")
async def root():
    if INDEX_FILE.exists():
        return FileResponse(INDEX_FILE)
    return JSONResponse(
        status_code=404,
        content={"detail": "Frontend index.html not found"},
    )


@app.get("/{full_path:path}")
async def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        return JSONResponse(
            status_code=404,
            content={"detail": "API route not found"},
        )

    if INDEX_FILE.exists():
        return FileResponse(INDEX_FILE)

    return JSONResponse(
        status_code=404,
        content={"detail": "Frontend index.html not found"},
    )