"""
Runner site registry service.

Responsibilities:
- load site configuration from sites.json
- normalize site records
- provide lookup helpers for Runner services and APIs

Runner uses this registry as the source of truth for:
- available sites
- site base URLs
- whether a site is enabled

This module does NOT call site APIs directly.
It only manages site metadata loaded from config.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.core.settings import SITES_CONFIG_PATH


# ---------------------------------------------------------------------
# Site registry
# ---------------------------------------------------------------------
class SiteRegistry:
    """
    Load and serve site configuration from sites.json.
    """

    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self._sites: list[dict[str, Any]] = []

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------
    def _normalize_site(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Normalize one raw site record.

        Supported input keys:
        - id / site_id
        - name / site_name
        - base_url
        - enabled
        - models
        - treatments
        - meta

        Notes:
        - unknown extra keys are preserved
        - base_url is stripped of trailing '/'
        """
        site_id = str(raw.get("id") or raw.get("site_id") or "").strip()
        if not site_id:
            raise ValueError("Site record missing required field: id")

        name = str(raw.get("name") or raw.get("site_name") or site_id).strip()
        base_url = str(raw.get("base_url") or "").strip().rstrip("/")

        enabled = raw.get("enabled", True)
        enabled = bool(enabled)

        models = raw.get("models", [])
        if not isinstance(models, list):
            models = []

        treatments = raw.get("treatments", [])
        if not isinstance(treatments, list):
            treatments = []

        meta = raw.get("meta", {})
        if not isinstance(meta, dict):
            meta = {}

        normalized = dict(raw)
        normalized.update(
            {
                "id": site_id,
                "site_id": site_id,
                "name": name,
                "site_name": name,
                "base_url": base_url,
                "enabled": enabled,
                "models": models,
                "treatments": treatments,
                "meta": meta,
            }
        )
        return normalized

    def _load_raw(self) -> list[dict[str, Any]]:
        """
        Read raw site list from disk.
        """
        if not self.config_path.exists():
            return []

        data = json.loads(self.config_path.read_text(encoding="utf-8"))
        sites = data.get("sites", [])

        if not isinstance(sites, list):
            return []

        out: list[dict[str, Any]] = []
        for item in sites:
            if isinstance(item, dict):
                out.append(item)
        return out

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------
    def load(self) -> None:
        """
        Load site registry from disk.

        Invalid site records are skipped.
        """
        raw_sites = self._load_raw()
        normalized_sites: list[dict[str, Any]] = []

        for raw in raw_sites:
            try:
                normalized_sites.append(self._normalize_site(raw))
            except Exception:
                # Keep registry robust:
                # skip broken records instead of breaking Runner startup.
                continue

        self._sites = normalized_sites

    def reload(self) -> None:
        """
        Reload site registry from disk.
        """
        self.load()

    def list_sites(self, enabled_only: bool = True) -> list[dict[str, Any]]:
        """
        Return site records.

        Args:
        - enabled_only: if True, return only enabled sites
        """
        if not enabled_only:
            return [dict(site) for site in self._sites]

        return [dict(site) for site in self._sites if site.get("enabled", True)]

    def list_site_ids(self, enabled_only: bool = True) -> list[str]:
        """
        Return site IDs only.
        """
        return [site["id"] for site in self.list_sites(enabled_only=enabled_only)]

    def get_site(self, site_id: str) -> dict[str, Any] | None:
        """
        Return one site config by site id.

        Matching is performed against:
        - id
        - site_id
        """
        site_id = str(site_id or "").strip()
        if not site_id:
            return None

        for site in self._sites:
            if site.get("id") == site_id or site.get("site_id") == site_id:
                return dict(site)
        return None

    def require_site(self, site_id: str, enabled_only: bool = True) -> dict[str, Any]:
        """
        Return one site config or raise ValueError.

        Args:
        - enabled_only: if True, disabled sites are treated as unavailable
        """
        site = self.get_site(site_id)
        if site is None:
            raise ValueError(f"Site not found: {site_id}")

        if enabled_only and not site.get("enabled", True):
            raise ValueError(f"Site is disabled: {site_id}")

        return site

    def get_site_base_url(self, site_id: str, enabled_only: bool = True) -> str:
        """
        Return normalized site base_url or raise ValueError.
        """
        site = self.require_site(site_id, enabled_only=enabled_only)
        base_url = str(site.get("base_url") or "").strip().rstrip("/")
        if not base_url:
            raise ValueError(f"Site base_url missing: {site_id}")
        return base_url

    def has_site(self, site_id: str, enabled_only: bool = False) -> bool:
        """
        Return whether a site exists.

        Args:
        - enabled_only: if True, only enabled sites count
        """
        site = self.get_site(site_id)
        if site is None:
            return False
        if enabled_only and not site.get("enabled", True):
            return False
        return True

    def to_dict(self, enabled_only: bool = False) -> dict[str, Any]:
        """
        Export registry content as dict.
        """
        return {"sites": self.list_sites(enabled_only=enabled_only)}


# ---------------------------------------------------------------------
# Singleton registry
# ---------------------------------------------------------------------
registry = SiteRegistry(SITES_CONFIG_PATH)