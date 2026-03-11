"""
Portal settings.

Portal should know:
- where its own DB is
- where Runner is
- its own secret key

Portal should NOT directly depend on site service URLs.
"""

import os


RUNNER_SERVICE_URL = os.getenv("RUNNER_SERVICE_URL", "http://localhost:8001")

PORTAL_DB_PATH = os.getenv("PORTAL_DB_PATH", "./portal.db")
PORTAL_SECRET_KEY = os.getenv("PORTAL_SECRET_KEY", "dev-secret-change-me")

# ---------------------------------------------------------
# Setup / development admin bootstrap
# ---------------------------------------------------------
# Recommended for local development only:
# AUTO_CREATE_DEV_ADMIN=true
# DEV_ADMIN_USERNAME=admin
# DEV_ADMIN_PASSWORD=admin123
#
# For production:
# keep AUTO_CREATE_DEV_ADMIN=false
# and use /api/setup/init once.
# ---------------------------------------------------------
AUTO_CREATE_DEV_ADMIN = os.getenv("AUTO_CREATE_DEV_ADMIN", "true").lower() == "true"
DEV_ADMIN_USERNAME = os.getenv("DEV_ADMIN_USERNAME", "admin")
DEV_ADMIN_PASSWORD = os.getenv("DEV_ADMIN_PASSWORD", "admin123")