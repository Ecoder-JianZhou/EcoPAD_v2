from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _artifact_output_type(item: dict[str, Any]) -> str:
    """
    Resolve output type from artifact.

    Priority:
    1. item["output_type"]
    2. item["series_type"]                  # backward compatibility
    3. item["metadata"]["output_type"]
    4. item["metadata"]["series_type"]     # backward compatibility
    """
    if not isinstance(item, dict):
        return ""

    direct_output_type = _normalize_text(item.get("output_type"))
    if direct_output_type:
        return direct_output_type

    direct_series_type = _normalize_text(item.get("series_type"))
    if direct_series_type:
        return direct_series_type

    metadata = item.get("metadata")
    if isinstance(metadata, dict):
        meta_output_type = _normalize_text(metadata.get("output_type"))
        if meta_output_type:
            return meta_output_type

        meta_series_type = _normalize_text(metadata.get("series_type"))
        if meta_series_type:
            return meta_series_type

    return ""


def find_parameter_artifact(
    manifest: dict[str, Any],
    *,
    artifact_type: str,
    model: str,
    treatment: str,
    output_type: str = "",
) -> dict[str, Any]:
    """
    Find one parameter artifact from manifest.

    Matching keys:
    - artifact_type
    - model_id
    - treatment
    - optional output_type
    """
    artifacts = manifest.get("artifacts") or []
    if not isinstance(artifacts, list):
        artifacts = []

    artifact_type = _normalize_text(artifact_type)
    model = _normalize_text(model)
    treatment = _normalize_text(treatment)
    output_type = _normalize_text(output_type)

    candidates: list[dict[str, Any]] = []

    for item in artifacts:
        if not isinstance(item, dict):
            continue

        if _normalize_text(item.get("artifact_type")) != artifact_type:
            continue

        if _normalize_text(item.get("model_id")) != model:
            continue

        if _normalize_text(item.get("treatment")) != treatment:
            continue

        candidates.append(item)

    if not candidates:
        raise HTTPException(
            404,
            f"parameter artifact not found for artifact_type={artifact_type}, "
            f"model={model}, treatment={treatment}",
        )

    if not output_type:
        return candidates[0]

    for item in candidates:
        if _artifact_output_type(item) == output_type:
            return item

    available_output_types = sorted(
        {ot for ot in (_artifact_output_type(x) for x in candidates) if ot}
    )

    raise HTTPException(
        404,
        f"parameter artifact not found for artifact_type={artifact_type}, "
        f"model={model}, treatment={treatment}, output_type={output_type}. "
        f"Available output_type values: {available_output_types or '[]'}",
    )


def _read_json_file(path: Path) -> Any:
    if not path.exists():
        raise HTTPException(404, f"{path.name} not found")

    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as ex:
        raise HTTPException(500, f"Failed to read json: {ex}")


def _read_csv_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise HTTPException(404, f"{path.name} not found")

    try:
        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]
    except Exception as ex:
        raise HTTPException(500, f"Failed to read csv: {ex}")


def read_parameter_artifact(run_dir: Path, artifact: dict[str, Any]) -> Any:
    """
    Read parameter artifact content from one manifest artifact row.

    Supported media types:
    - application/json -> dict or list
    - text/csv / application/csv -> list[dict]
    """
    rel_path = _normalize_text(artifact.get("rel_path"))
    media_type = _normalize_text(artifact.get("media_type"))

    if not rel_path:
        raise HTTPException(500, "artifact rel_path missing")

    path = run_dir / rel_path
    if not path.exists():
        raise HTTPException(404, f"artifact file not found: {rel_path}")

    if media_type == "application/json":
        return _read_json_file(path)

    if media_type in ("text/csv", "application/csv"):
        return _read_csv_file(path)

    raise HTTPException(
        500,
        f"unsupported parameter artifact media_type={media_type!r}",
    )