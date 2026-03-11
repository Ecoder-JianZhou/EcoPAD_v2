"""
Runner settings.

Responsibilities:
- read environment variables
- provide centralized configuration values
- keep defaults simple for local development
"""

from __future__ import annotations

import os


# ---------------------------------------------------------------------
# Basic paths
# ---------------------------------------------------------------------
RUNNER_DB_PATH = os.getenv("RUNNER_DB_PATH", "./db/runner.db")
SITES_CONFIG_PATH = os.getenv("SITES_CONFIG_PATH", "./config/sites.json")


# ---------------------------------------------------------------------
# Site communication
# ---------------------------------------------------------------------
# Timeout (seconds) for Runner <-> Site HTTP requests.
SITE_REQUEST_TIMEOUT = float(os.getenv("SITE_REQUEST_TIMEOUT", "120"))


# ---------------------------------------------------------------------
# Runner service
# ---------------------------------------------------------------------
RUNNER_HOST = os.getenv("RUNNER_HOST", "0.0.0.0")
RUNNER_PORT = int(os.getenv("RUNNER_PORT", "8001"))


# ---------------------------------------------------------------------
# Optional future settings
# ---------------------------------------------------------------------
# Cleanup / scheduler related placeholders for future use.
RUNNER_LOG_LEVEL = os.getenv("RUNNER_LOG_LEVEL", "info")
AUTO_FORECAST_ENABLED = os.getenv("AUTO_FORECAST_ENABLED", "false").lower() == "true"
CLEANUP_ENABLED = os.getenv("CLEANUP_ENABLED", "false").lower() == "true"