from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException


def find_parameter_artifact(
    manifest: dict[str, Any],
    *,
    artifact_type: str,
    model: str,
    treatment: str,
) -> dict[str, Any]:
    artifacts = manifest.get("artifacts") or []
    if not isinstance(artifacts, list):
        artifacts = []

    for item in artifacts:
        if not isinstance(item, dict):
            continue

        if str(item.get("artifact_type") or "") != artifact_type:
            continue
        if str(item.get("model_id") or "") != model:
            continue
        if str(item.get("treatment") or "") != treatment:
            continue

        return item

    raise HTTPException(
        404,
        f"parameter artifact not found for artifact_type={artifact_type}, "
        f"model={model}, treatment={treatment}",
    )


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise HTTPException(404, f"{path.name} not found")

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as ex:
        raise HTTPException(500, f"Failed to read json: {ex}")

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid json format")

    return data


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
    rel_path = str(artifact.get("rel_path") or "").strip()
    media_type = str(artifact.get("media_type") or "").strip()

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