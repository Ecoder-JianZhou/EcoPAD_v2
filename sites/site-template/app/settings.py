from __future__ import annotations

from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
MODELS_DIR = CONFIG_DIR / "models"
WORKSPACE_DIR = BASE_DIR / "workspace"
RUNS_DIR = WORKSPACE_DIR / "runs"

SITE_CONFIG_PATH = Path(os.getenv("SITE_CONFIG_PATH", str(CONFIG_DIR / "site.json")))


def ensure_workspace() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)


def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id