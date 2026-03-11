"""
Runner cleanup service.

Responsibilities:
- find runs eligible for cleanup
- protect published/latest forecast runs
- delete run_outputs rows for expired runs
- write cleanup audit logs
- keep runs table as long-term platform truth

Current design:
- cleanup is logical cleanup first
- Runner removes its own artifact registry rows
- Runner does NOT delete remote site files yet
- runs row remains as long-term truth
- runs row gets cleanup_status / cleaned_at markers
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.db import get_db, now_iso


# ---------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------
DEFAULT_TTL_DAYS = {
    "ephemeral": 7,
    "normal": 90,
    "published": None,  # never auto-clean
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _parse_iso(value: str | None) -> datetime | None:
    """
    Parse ISO datetime safely.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _cutoff_iso(days: int) -> str:
    """
    Return cutoff ISO string for utcnow - days.
    """
    dt = datetime.now(timezone.utc) - timedelta(days=int(days))
    return dt.isoformat()


def _row_to_dict(row: Any) -> dict[str, Any] | None:
    """
    Convert sqlite row to dict.
    """
    if row is None:
        return None
    return dict(row)


# ---------------------------------------------------------------------
# Cleanup log
# ---------------------------------------------------------------------
async def log_cleanup_action(
    *,
    run_id: str,
    action: str,
    target_path: str = "",
    detail: str = "",
) -> None:
    """
    Insert one cleanup audit log row.
    """
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO cleanup_log (
                run_id,
                action,
                target_path,
                detail,
                created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                action,
                target_path or "",
                detail or "",
                now_iso(),
            ),
        )
        await db.commit()
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Candidate selection
# ---------------------------------------------------------------------
async def list_cleanup_candidates(
    *,
    ttl_days_ephemeral: int = 7,
    ttl_days_normal: int = 90,
    site_id: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """
    Return runs eligible for cleanup.

    Rules:
    - only terminal runs: done / failed / cancelled
    - retention_class = ephemeral and older than ttl_days_ephemeral
    - retention_class = normal and older than ttl_days_normal
    - retention_class = published is never auto-cleaned
    - runs currently referenced by forecast_registry.is_latest = 1 are protected
    - already logically cleaned runs are skipped
    """
    cutoff_ephemeral = _cutoff_iso(ttl_days_ephemeral)
    cutoff_normal = _cutoff_iso(ttl_days_normal)

    where = [
        "r.status IN ('done', 'failed', 'cancelled')",
        "COALESCE(r.cleanup_status, '') != 'cleaned'",
        """
        (
            (r.retention_class='ephemeral' AND COALESCE(r.finished_at, r.updated_at, r.created_at) < ?)
            OR
            (r.retention_class='normal' AND COALESCE(r.finished_at, r.updated_at, r.created_at) < ?)
        )
        """,
        """
        NOT EXISTS (
            SELECT 1
            FROM forecast_registry fr
            WHERE fr.source_run_id = r.id
              AND fr.is_latest = 1
        )
        """,
    ]
    params: list[Any] = [cutoff_ephemeral, cutoff_normal]

    if site_id:
        where.append("r.site_id=?")
        params.append(site_id)

    sql = f"""
    SELECT r.*
    FROM runs r
    WHERE {" AND ".join(where)}
    ORDER BY COALESCE(r.finished_at, r.updated_at, r.created_at) ASC
    LIMIT ?
    """
    params.append(max(1, int(limit)))

    db = await get_db()
    try:
        cur = await db.execute(sql, tuple(params))
        rows = await cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


# ---------------------------------------------------------------------
# Cleanup execution
# ---------------------------------------------------------------------
async def cleanup_one_run(
    run_id: str,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Perform logical cleanup for one run.

    Current behavior:
    - delete run_outputs rows
    - keep runs row
    - write cleanup_log
    - mark runs.cleanup_status / cleaned_at

    Does NOT:
    - delete forecast_registry rows
    - delete remote site files
    """
    db = await get_db()
    try:
        cur = await db.execute(
            """
            SELECT *
            FROM runs
            WHERE id=?
            """,
            (run_id,),
        )
        run = await cur.fetchone()
        if run is None:
            raise ValueError(f"Run not found: {run_id}")

        run_obj = dict(run)

        # Protect latest published forecast sources
        cur2 = await db.execute(
            """
            SELECT COUNT(*) AS n
            FROM forecast_registry
            WHERE source_run_id=?
              AND is_latest=1
            """,
            (run_id,),
        )
        row2 = await cur2.fetchone()
        is_latest_forecast = int(row2["n"] if row2 else 0) > 0
        if is_latest_forecast:
            return {
                "run_id": run_id,
                "cleaned": False,
                "reason": "protected_latest_forecast",
            }

        if run_obj["retention_class"] == "published":
            return {
                "run_id": run_id,
                "cleaned": False,
                "reason": "published_retention",
            }

        if str(run_obj.get("cleanup_status") or "") == "cleaned":
            return {
                "run_id": run_id,
                "cleaned": False,
                "reason": "already_cleaned",
            }

        cur3 = await db.execute(
            """
            SELECT COUNT(*) AS n
            FROM run_outputs
            WHERE run_id=?
            """,
            (run_id,),
        )
        row3 = await cur3.fetchone()
        n_outputs = int(row3["n"] if row3 else 0)

        if dry_run:
            return {
                "run_id": run_id,
                "cleaned": False,
                "dry_run": True,
                "run_outputs_to_delete": n_outputs,
            }

        cleaned_at = now_iso()

        await db.execute("BEGIN")

        await db.execute(
            """
            DELETE FROM run_outputs
            WHERE run_id=?
            """,
            (run_id,),
        )

        await db.execute(
            """
            UPDATE runs
            SET cleanup_status=?,
                cleaned_at=?,
                updated_at=?
            WHERE id=?
            """,
            ("cleaned", cleaned_at, cleaned_at, run_id),
        )

        await db.execute(
            """
            INSERT INTO cleanup_log (
                run_id,
                action,
                target_path,
                detail,
                created_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                "delete_run_outputs",
                "",
                f"Deleted {n_outputs} run_outputs rows",
                cleaned_at,
            ),
        )

        await db.commit()

        return {
            "run_id": run_id,
            "cleaned": True,
            "deleted_run_outputs": n_outputs,
            "cleaned_at": cleaned_at,
        }

    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


async def run_cleanup(
    *,
    ttl_days_ephemeral: int = 7,
    ttl_days_normal: int = 90,
    site_id: str | None = None,
    limit: int = 500,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Run batch cleanup.

    Returns summary:
    {
      "candidates": 10,
      "cleaned": 8,
      "skipped": 2,
      "results": [...]
    }
    """
    candidates = await list_cleanup_candidates(
        ttl_days_ephemeral=ttl_days_ephemeral,
        ttl_days_normal=ttl_days_normal,
        site_id=site_id,
        limit=limit,
    )

    results: list[dict[str, Any]] = []
    cleaned = 0
    skipped = 0

    for run in candidates:
        result = await cleanup_one_run(
            run["id"],
            dry_run=dry_run,
        )
        results.append(result)

        if result.get("cleaned"):
            cleaned += 1
        else:
            skipped += 1

    return {
        "candidates": len(candidates),
        "cleaned": cleaned,
        "skipped": skipped,
        "dry_run": dry_run,
        "results": results,
    }