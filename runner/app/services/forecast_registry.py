"""
Runner forecast registry service.

Responsibilities:
- publish a new forecast version
- publish multiple forecast items from a manifest
- mark old latest version as not latest
- query latest forecast records
- return site-level forecast summary
- list published latest forecast items
- provide helper queries for auto-forecast parameter chains

This module owns Forecast-page truth in Runner DB.
It does NOT execute models and does NOT read raw site files directly.
"""

from __future__ import annotations

import json
from typing import Any

from app.core.db import get_db, now_iso


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _as_json_text(value: dict[str, Any] | list[Any] | None) -> str:
    """
    Convert Python dict/list to compact JSON string.
    """
    if value is None:
        return "{}"
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _parse_json_text(value: Any) -> Any:
    """
    Parse JSON text safely.

    Rules:
    - None -> {}
    - dict/list -> itself
    - invalid json string -> original string
    """
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except Exception:
        return value


def _row_to_dict(row: Any) -> dict[str, Any] | None:
    """
    Convert sqlite row to plain dict and expand JSON columns when present.
    """
    if row is None:
        return None

    obj = dict(row)

    if "source_ref_json" in obj:
        obj["source_ref"] = _parse_json_text(obj.pop("source_ref_json"))

    if "metadata_json" in obj:
        obj["metadata"] = _parse_json_text(obj.pop("metadata_json"))

    if "payload_json" in obj:
        obj["payload"] = _parse_json_text(obj.pop("payload_json"))

    if "config_json" in obj:
        obj["config"] = _parse_json_text(obj.pop("config_json"))

    return obj


def _normalize_text(value: Any) -> str:
    """
    Normalize any value into a stripped string.
    """
    return str(value or "").strip()


def _normalize_flag(value: Any, default: int = 1) -> int:
    """
    Normalize value into 0/1 integer flag.
    """
    if value is None:
        return int(default)
    return 1 if int(value) else 0


# ---------------------------------------------------------------------
# Core publish internals
# ---------------------------------------------------------------------
async def _publish_one_in_tx(
    db,
    *,
    site_id: str,
    model_id: str,
    variable: str,
    treatment: str,
    source_run_id: str,
    data_path: str = "",
    obs_path: str = "",
    source_ref: dict[str, Any] | None = None,
    is_published: int = 1,
) -> int:
    """
    Publish one forecast row inside an existing transaction.

    Behavior:
    - old latest row for the same (site, model, variable, treatment) becomes not-latest
    - new row is inserted as latest

    Returns inserted row id.
    """
    now = now_iso()

    await db.execute(
        """
        UPDATE forecast_registry
        SET is_latest=0
        WHERE site_id=?
          AND model_id=?
          AND variable=?
          AND treatment=?
          AND is_latest=1
        """,
        (site_id, model_id, variable, treatment),
    )

    cur = await db.execute(
        """
        INSERT INTO forecast_registry (
            site_id,
            model_id,
            variable,
            treatment,
            source_run_id,
            data_path,
            obs_path,
            source_ref_json,
            updated_at,
            is_latest,
            is_published
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            site_id,
            model_id,
            variable,
            treatment,
            source_run_id,
            data_path,
            obs_path,
            _as_json_text(source_ref or {}),
            now,
            _normalize_flag(is_published, 1),
        ),
    )
    return int(cur.lastrowid)


# ---------------------------------------------------------------------
# Public publish APIs
# ---------------------------------------------------------------------
async def publish_forecast(
    *,
    site_id: str,
    model_id: str,
    variable: str,
    treatment: str,
    source_run_id: str,
    data_path: str = "",
    obs_path: str | None = None,
    source_ref: dict[str, Any] | None = None,
    is_published: int = 1,
) -> dict[str, Any]:
    """
    Publish one new forecast version and mark previous latest as old.
    """
    site_id = _normalize_text(site_id)
    model_id = _normalize_text(model_id)
    variable = _normalize_text(variable)
    treatment = _normalize_text(treatment)
    source_run_id = _normalize_text(source_run_id)

    if not site_id or not model_id or not variable or not treatment or not source_run_id:
        raise ValueError("site_id, model_id, variable, treatment, and source_run_id are required")

    db = await get_db()
    try:
        await db.execute("BEGIN")
        row_id = await _publish_one_in_tx(
            db,
            site_id=site_id,
            model_id=model_id,
            variable=variable,
            treatment=treatment,
            source_run_id=source_run_id,
            data_path=_normalize_text(data_path),
            obs_path=_normalize_text(obs_path),
            source_ref=source_ref,
            is_published=is_published,
        )
        await db.commit()

        cur = await db.execute(
            """
            SELECT *
            FROM forecast_registry
            WHERE id=?
            """,
            (row_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to fetch inserted forecast row")

        return _row_to_dict(row) or {}
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


async def publish_forecasts(
    *,
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Publish multiple forecast items in one transaction.

    Each item requires:
    - site_id
    - model_id
    - variable
    - treatment
    - source_run_id

    Optional:
    - data_path
    - obs_path
    - source_ref
    - is_published
    """
    if not items:
        return []

    db = await get_db()
    inserted_ids: list[int] = []

    try:
        await db.execute("BEGIN")

        for item in items:
            site_id = _normalize_text(item.get("site_id"))
            model_id = _normalize_text(item.get("model_id"))
            variable = _normalize_text(item.get("variable"))
            treatment = _normalize_text(item.get("treatment"))
            source_run_id = _normalize_text(item.get("source_run_id"))

            if not site_id or not model_id or not variable or not treatment or not source_run_id:
                raise ValueError(
                    "Each publish item requires site_id, model_id, variable, treatment, source_run_id"
                )

            row_id = await _publish_one_in_tx(
                db,
                site_id=site_id,
                model_id=model_id,
                variable=variable,
                treatment=treatment,
                source_run_id=source_run_id,
                data_path=_normalize_text(item.get("data_path")),
                obs_path=_normalize_text(item.get("obs_path")),
                source_ref=item.get("source_ref"),
                is_published=_normalize_flag(item.get("is_published"), 1),
            )
            inserted_ids.append(row_id)

        await db.commit()

        out: list[dict[str, Any]] = []
        for row_id in inserted_ids:
            cur = await db.execute(
                """
                SELECT *
                FROM forecast_registry
                WHERE id=?
                """,
                (row_id,),
            )
            row = await cur.fetchone()
            if row:
                out.append(_row_to_dict(row) or {})
        return out
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


async def publish_from_manifest(
    *,
    site_id: str,
    source_run_id: str,
    manifest: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Publish forecast rows declared by one site manifest.

    Expected shape:
    {
      "forecast_registry": [
        {
          "model_id": "TECO-SPRUCE",
          "variable": "GPP",
          "treatment": "W0.00_CO2_000",
          "data_path": "/path/to/file.json",
          "obs_path": "",
          "source_ref": {...},
          "is_published": 1
        }
      ]
    }
    """
    site_id = _normalize_text(site_id)
    source_run_id = _normalize_text(source_run_id)

    if not site_id or not source_run_id:
        raise ValueError("site_id and source_run_id are required")

    raw_items = manifest.get("forecast_registry") or []
    items: list[dict[str, Any]] = []

    for item in raw_items:
        if not isinstance(item, dict):
            continue

        model_id = _normalize_text(item.get("model_id"))
        variable = _normalize_text(item.get("variable"))
        treatment = _normalize_text(item.get("treatment"))

        if not model_id or not variable or not treatment:
            continue

        items.append(
            {
                "site_id": site_id,
                "model_id": model_id,
                "variable": variable,
                "treatment": treatment,
                "source_run_id": source_run_id,
                "data_path": _normalize_text(item.get("data_path")),
                "obs_path": _normalize_text(item.get("obs_path")),
                "source_ref": item.get("source_ref") or _parse_json_text(item.get("source_ref_json")) or {},
                "is_published": _normalize_flag(item.get("is_published"), 1),
            }
        )

    return await publish_forecasts(items=items)


# ---------------------------------------------------------------------
# Query latest
# ---------------------------------------------------------------------
async def get_latest_forecast(
    *,
    site_id: str,
    variable: str,
    treatment: str,
    model_id: str | None = None,
    published_only: bool = True,
) -> dict[str, Any] | None:
    """
    Return latest forecast row for one site / variable / treatment.
    """
    db = await get_db()
    try:
        where = [
            "site_id=?",
            "variable=?",
            "treatment=?",
            "is_latest=1",
        ]
        params: list[Any] = [site_id, variable, treatment]

        if model_id:
            where.append("model_id=?")
            params.append(model_id)

        if published_only:
            where.append("is_published=1")

        sql = f"""
        SELECT *
        FROM forecast_registry
        WHERE {" AND ".join(where)}
        ORDER BY updated_at DESC
        LIMIT 1
        """

        cur = await db.execute(sql, tuple(params))
        row = await cur.fetchone()
        return _row_to_dict(row)
    finally:
        await db.close()


async def list_latest_forecasts(
    *,
    site_id: str,
    published_only: bool = True,
    model_id: str | None = None,
    variable: str | None = None,
    treatment: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """
    List latest forecast rows for one site with optional filters.
    """
    db = await get_db()
    try:
        where = ["site_id=?", "is_latest=1"]
        params: list[Any] = [site_id]

        if published_only:
            where.append("is_published=1")

        if model_id:
            where.append("model_id=?")
            params.append(model_id)

        if variable:
            where.append("variable=?")
            params.append(variable)

        if treatment:
            where.append("treatment=?")
            params.append(treatment)

        sql = f"""
        SELECT *
        FROM forecast_registry
        WHERE {" AND ".join(where)}
        ORDER BY updated_at DESC
        LIMIT ?
        """
        params.append(max(1, int(limit)))

        cur = await db.execute(sql, tuple(params))
        rows = await cur.fetchall()
        return [_row_to_dict(r) or {} for r in rows]
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------
async def get_forecast_summary(site_id: str) -> dict[str, Any]:
    """
    Return site-level summary for Forecast page.
    """
    db = await get_db()
    try:
        cur1 = await db.execute(
            """
            SELECT MAX(updated_at) AS latest_update
            FROM forecast_registry
            WHERE site_id=?
              AND is_latest=1
              AND is_published=1
            """,
            (site_id,),
        )
        row1 = await cur1.fetchone()

        cur2 = await db.execute(
            """
            SELECT COUNT(*) AS n_items
            FROM forecast_registry
            WHERE site_id=?
              AND is_latest=1
              AND is_published=1
            """,
            (site_id,),
        )
        row2 = await cur2.fetchone()

        cur3 = await db.execute(
            """
            SELECT fr.*
            FROM forecast_registry fr
            JOIN runs r ON r.id = fr.source_run_id
            WHERE fr.site_id=?
              AND fr.is_latest=1
              AND fr.is_published=1
              AND r.task_type='auto_forecast'
            ORDER BY fr.updated_at DESC
            LIMIT 1
            """,
            (site_id,),
        )
        row3 = await cur3.fetchone()

        cur4 = await db.execute(
            """
            SELECT DISTINCT model_id
            FROM forecast_registry
            WHERE site_id=?
              AND is_latest=1
              AND is_published=1
            ORDER BY model_id
            """,
            (site_id,),
        )
        models = [r["model_id"] for r in await cur4.fetchall()]

        cur5 = await db.execute(
            """
            SELECT DISTINCT variable
            FROM forecast_registry
            WHERE site_id=?
              AND is_latest=1
              AND is_published=1
            ORDER BY variable
            """,
            (site_id,),
        )
        variables = [r["variable"] for r in await cur5.fetchall()]

        cur6 = await db.execute(
            """
            SELECT DISTINCT treatment
            FROM forecast_registry
            WHERE site_id=?
              AND is_latest=1
              AND is_published=1
            ORDER BY treatment
            """,
            (site_id,),
        )
        treatments = [r["treatment"] for r in await cur6.fetchall()]

        return {
            "site_id": site_id,
            "latest_update": row1["latest_update"] if row1 else None,
            "published_items": int(row2["n_items"]) if row2 else 0,
            "models": models,
            "variables": variables,
            "treatments": treatments,
            "latest_auto_forecast": _row_to_dict(row3),
        }
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Publish / visibility management
# ---------------------------------------------------------------------
async def set_forecast_published(
    forecast_id: int,
    *,
    is_published: int,
) -> dict[str, Any]:
    """
    Update publication visibility for one forecast row.
    """
    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE forecast_registry
            SET is_published=?,
                updated_at=?
            WHERE id=?
            """,
            (_normalize_flag(is_published), now_iso(), int(forecast_id)),
        )
        await db.commit()

        cur = await db.execute(
            """
            SELECT *
            FROM forecast_registry
            WHERE id=?
            """,
            (int(forecast_id),),
        )
        row = await cur.fetchone()
        if row is None:
            raise ValueError(f"Forecast row not found: {forecast_id}")
        return _row_to_dict(row) or {}
    finally:
        await db.close()


async def unlatest_forecast_series(
    *,
    site_id: str,
    model_id: str,
    variable: str,
    treatment: str,
) -> int:
    """
    Mark current latest rows as not latest for one series.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            UPDATE forecast_registry
            SET is_latest=0
            WHERE site_id=?
              AND model_id=?
              AND variable=?
              AND treatment=?
              AND is_latest=1
            """,
            (site_id, model_id, variable, treatment),
        )
        await db.commit()
        return int(cur.rowcount or 0)
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------
async def get_forecast_by_id(forecast_id: int) -> dict[str, Any] | None:
    """
    Return one forecast row by primary key.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT *
            FROM forecast_registry
            WHERE id=?
            """,
            (int(forecast_id),),
        )
        row = await cur.fetchone()
        return _row_to_dict(row)
    finally:
        await db.close()


async def list_forecasts_for_run(
    source_run_id: str,
    *,
    latest_only: bool = False,
) -> list[dict[str, Any]]:
    """
    List forecast rows produced from one source run.
    """
    db = await get_db()
    try:
        sql = """
        SELECT *
        FROM forecast_registry
        WHERE source_run_id=?
        """
        params: list[Any] = [source_run_id]

        if latest_only:
            sql += " AND is_latest=1"

        sql += " ORDER BY updated_at DESC"

        cur = await db.execute(sql, tuple(params))
        rows = await cur.fetchall()
        return [_row_to_dict(r) or {} for r in rows]
    finally:
        await db.close()


async def list_latest_variables(site_id: str) -> list[str]:
    """
    List distinct latest published variables for one site.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT DISTINCT variable
            FROM forecast_registry
            WHERE site_id=?
              AND is_latest=1
              AND is_published=1
            ORDER BY variable
            """,
            (site_id,),
        )
        rows = await cur.fetchall()
        return [r["variable"] for r in rows]
    finally:
        await db.close()


async def list_latest_treatments(site_id: str) -> list[str]:
    """
    List distinct latest published treatments for one site.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT DISTINCT treatment
            FROM forecast_registry
            WHERE site_id=?
              AND is_latest=1
              AND is_published=1
            ORDER BY treatment
            """,
            (site_id,),
        )
        rows = await cur.fetchall()
        return [r["treatment"] for r in rows]
    finally:
        await db.close()


async def list_latest_models(site_id: str) -> list[str]:
    """
    List distinct latest published models for one site.
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT DISTINCT model_id
            FROM forecast_registry
            WHERE site_id=?
              AND is_latest=1
              AND is_published=1
            ORDER BY model_id
            """,
            (site_id,),
        )
        rows = await cur.fetchall()
        return [r["model_id"] for r in rows]
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Auto-forecast chain helpers
# ---------------------------------------------------------------------
async def get_latest_auto_forecast_run_for_series(
    *,
    site_id: str,
    model_id: str,
    treatment: str,
    variable: str,
) -> dict[str, Any] | None:
    """
    Resolve the latest auto-forecast run for one published forecast series.

    This helper joins forecast_registry -> runs and returns the run row.

    Why this matters:
    - forecast_registry tells us which run currently powers Forecast page
    - runs carries schedule metadata such as scheduled_task_id
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT r.*
            FROM forecast_registry fr
            JOIN runs r
              ON r.id = fr.source_run_id
            WHERE fr.site_id=?
              AND fr.model_id=?
              AND fr.treatment=?
              AND fr.variable=?
              AND fr.is_latest=1
              AND fr.is_published=1
              AND r.task_type='auto_forecast'
            ORDER BY fr.updated_at DESC
            LIMIT 1
            """,
            (site_id, model_id, treatment, variable),
        )
        row = await cur.fetchone()
        return _row_to_dict(row)
    finally:
        await db.close()


async def get_latest_parameter_snapshot_for_schedule(
    *,
    scheduled_task_id: int,
    model_id: str,
    artifact_type: str = "parameter_summary",
) -> dict[str, Any] | None:
    """
    Return the newest parameter artifact for one recurring schedule chain.

    Source:
    - runs table
    - run_outputs table

    Filters:
    - runs.scheduled_task_id = given schedule id
    - run_outputs.artifact_type = parameter_summary by default
    - model_id is matched through run_outputs.metadata.model_id when available,
      otherwise the parent run.model_id is accepted

    Result:
    - one latest artifact row with expanded metadata
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT
                ro.*,
                r.site_id,
                r.model_id AS run_model_id,
                r.task_type,
                r.status AS run_status,
                r.finished_at AS run_finished_at,
                r.updated_at AS run_updated_at,
                r.created_at AS run_created_at
            FROM run_outputs ro
            JOIN runs r
              ON r.id = ro.run_id
            WHERE r.scheduled_task_id=?
              AND ro.artifact_type=?
              AND (
                    json_extract(ro.metadata_json, '$.model_id') IS NULL
                    OR json_extract(ro.metadata_json, '$.model_id')=''
                    OR json_extract(ro.metadata_json, '$.model_id')=?
                    OR r.model_id=?
                  )
            ORDER BY
                COALESCE(r.finished_at, r.updated_at, r.created_at) DESC,
                ro.id DESC
            LIMIT 1
            """,
            (int(scheduled_task_id), artifact_type, model_id, model_id),
        )
        row = await cur.fetchone()
        return _row_to_dict(row)
    finally:
        await db.close()


async def list_parameter_history_for_schedule(
    *,
    scheduled_task_id: int,
    model_id: str,
    param_id: str,
    treatment: str = "",
    artifact_type: str = "parameter_summary",
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """
    Return parameter history across repeated auto-forecast runs in one schedule chain.

    Expected parameter_summary artifact metadata shape:
    {
      "model_id": "TECO-SPRUCE",
      "treatment": "W0.00_CO2_000",
      "parameters": {
        "vcmax": {"value": 42.1, "min": 10, "max": 80, ...},
        ...
      }
    }

    Returned rows are normalized into a list where each item contains:
    - run_id
    - created / updated / finished time
    - artifact metadata
    - extracted parameter entry under key "parameter"
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT
                ro.*,
                r.site_id,
                r.model_id AS run_model_id,
                r.task_type,
                r.status AS run_status,
                r.finished_at AS run_finished_at,
                r.updated_at AS run_updated_at,
                r.created_at AS run_created_at
            FROM run_outputs ro
            JOIN runs r
              ON r.id = ro.run_id
            WHERE r.scheduled_task_id=?
              AND ro.artifact_type=?
              AND (
                    json_extract(ro.metadata_json, '$.model_id') IS NULL
                    OR json_extract(ro.metadata_json, '$.model_id')=''
                    OR json_extract(ro.metadata_json, '$.model_id')=?
                    OR r.model_id=?
                  )
              AND (
                    ?=''
                    OR json_extract(ro.metadata_json, '$.treatment') IS NULL
                    OR json_extract(ro.metadata_json, '$.treatment')=''
                    OR json_extract(ro.metadata_json, '$.treatment')=?
                    OR ro.treatment=?
                  )
            ORDER BY
                COALESCE(r.finished_at, r.updated_at, r.created_at) ASC,
                ro.id ASC
            LIMIT ?
            """,
            (
                int(scheduled_task_id),
                artifact_type,
                model_id,
                model_id,
                treatment,
                treatment,
                treatment,
                max(1, int(limit)),
            ),
        )
        rows = await cur.fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            item = _row_to_dict(row) or {}
            metadata = _parse_json_text(item.get("metadata")) if "metadata" in item else {}
            if not isinstance(metadata, dict):
                metadata = {}

            parameters = metadata.get("parameters")
            if not isinstance(parameters, dict):
                parameters = {}

            out.append(
                {
                    **item,
                    "parameter": parameters.get(param_id),
                }
            )

        return out
    finally:
        await db.close()


async def get_parameter_hist_artifact_for_run(
    *,
    run_id: str,
    model_id: str,
    artifact_type: str = "parameter_posterior",
) -> dict[str, Any] | None:
    """
    Return one posterior / histogram artifact for a specific run.

    Typical artifact types:
    - parameter_posterior
    - parameter_hist

    Matching logic:
    - artifact_type must match exactly
    - if metadata.model_id exists, it should match requested model_id
    - otherwise fall back to the parent run.model_id
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT
                ro.*,
                r.site_id,
                r.model_id AS run_model_id,
                r.task_type,
                r.status AS run_status,
                r.finished_at AS run_finished_at,
                r.updated_at AS run_updated_at,
                r.created_at AS run_created_at
            FROM run_outputs ro
            JOIN runs r
              ON r.id = ro.run_id
            WHERE ro.run_id=?
              AND ro.artifact_type=?
              AND (
                    json_extract(ro.metadata_json, '$.model_id') IS NULL
                    OR json_extract(ro.metadata_json, '$.model_id')=''
                    OR json_extract(ro.metadata_json, '$.model_id')=?
                    OR r.model_id=?
                  )
            ORDER BY ro.id DESC
            LIMIT 1
            """,
            (run_id, artifact_type, model_id, model_id),
        )
        row = await cur.fetchone()
        return _row_to_dict(row)
    finally:
        await db.close()