from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException


# -----------------------------------------------------------------------------
# Canonical output types
# -----------------------------------------------------------------------------
SUPPORTED_OUTPUT_TYPES = (
    "simulation_without_da",
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
    "auto_forecast_with_da",
    "auto_forecast_without_da",
)


# backward-compat aliases
LEGACY_SERIES_ALIASES = {
    "simulate": "simulation_without_da",
    "simulation_with_da": "simulation_with_da",
    "forecast_with_da": "forecast_with_da",
    "forecast_without_da": "forecast_without_da",
}


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------
def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_requested_output_type(
    value: Any,
    default: str = "",
) -> str:
    """
    Normalize requested output type from either:
    - new output_type names
    - old series_type names
    """
    text = _normalize_text(value)
    if not text:
        return default

    if text in SUPPORTED_OUTPUT_TYPES:
        return text

    mapped = LEGACY_SERIES_ALIASES.get(text)
    if mapped:
        return mapped

    return default


def load_manifest(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "manifest.json"
    if not path.exists():
        raise HTTPException(404, "manifest not found")

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as ex:
        raise HTTPException(500, f"failed to read manifest: {ex}")

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid manifest format")

    return data


# -----------------------------------------------------------------------------
# Artifact output-type resolution
# -----------------------------------------------------------------------------
def _artifact_output_type(item: dict[str, Any]) -> str:
    """
    Resolve output type from artifact, supporting:
    - item["output_type"]                    (new)
    - item["series_type"]                    (compat)
    - item["forecast_mode"]                  (older compat)
    - item["metadata"]["output_type"]        (new)
    - item["metadata"]["series_type"]        (compat)
    - item["metadata"]["forecast_mode"]      (older compat)
    """
    direct = _normalize_text(item.get("output_type"))
    if direct:
        return _normalize_requested_output_type(direct, default="")

    compat_series = _normalize_text(item.get("series_type"))
    if compat_series:
        return _normalize_requested_output_type(compat_series, default="")

    legacy = _normalize_text(item.get("forecast_mode"))
    if legacy:
        return _normalize_requested_output_type(legacy, default="")

    metadata = item.get("metadata")
    if isinstance(metadata, dict):
        md_output_type = _normalize_text(metadata.get("output_type"))
        if md_output_type:
            return _normalize_requested_output_type(md_output_type, default="")

        md_series = _normalize_text(metadata.get("series_type"))
        if md_series:
            return _normalize_requested_output_type(md_series, default="")

        md_legacy = _normalize_text(metadata.get("forecast_mode"))
        if md_legacy:
            return _normalize_requested_output_type(md_legacy, default="")

    return ""


def _find_timeseries_artifact_from_artifacts(
    manifest: dict[str, Any],
    *,
    model: str,
    treatment: str,
    variable: str,
    output_type: str,
) -> dict[str, Any] | None:
    """
    Primary lookup from manifest["artifacts"].

    Matching rule:
    - artifact_type == "timeseries"
    - model_id / treatment / variable must match
    - output_type must match requested normalized type

    Legacy fallback:
    - if old artifact lacks output_type/series_type, only allow fallback
      when requested type is forecast_with_da or simulation_without_da
    """
    artifacts = manifest.get("artifacts") or []
    if not isinstance(artifacts, list):
        artifacts = []

    requested_output_type = _normalize_requested_output_type(output_type, default="")
    legacy_match: dict[str, Any] | None = None

    for item in artifacts:
        if not isinstance(item, dict):
            continue

        if _normalize_text(item.get("artifact_type")) != "timeseries":
            continue
        if _normalize_text(item.get("model_id")) != model:
            continue
        if _normalize_text(item.get("treatment")) != treatment:
            continue
        if _normalize_text(item.get("variable")) != variable:
            continue

        artifact_output_type = _artifact_output_type(item)

        if artifact_output_type:
            if requested_output_type and artifact_output_type == requested_output_type:
                return item
            continue

        if requested_output_type in {"forecast_with_da", "simulation_without_da"}:
            legacy_match = item

    return legacy_match


def _find_timeseries_artifact_from_outputs_index(
    manifest: dict[str, Any],
    *,
    model: str,
    treatment: str,
    variable: str,
    output_type: str,
) -> dict[str, Any] | None:
    """
    Fallback lookup from manifest["outputs"]["index"].

    New shape:
    {
      "<model>": {
        "<treatment>": {
          "<output_type>": {
            "<variable>": "outputs/..."
          }
        }
      }
    }

    Old compatibility shape:
    {
      "<model>": {
        "<treatment>": {
          "<variable>": "outputs/..."
        }
      }
    }
    """
    outputs = manifest.get("outputs") or {}
    if not isinstance(outputs, dict):
        return None

    index = outputs.get("index") or {}
    if not isinstance(index, dict):
        return None

    model_block = index.get(model)
    if not isinstance(model_block, dict):
        return None

    treatment_block = model_block.get(treatment)
    if not isinstance(treatment_block, dict):
        return None

    requested_output_type = _normalize_requested_output_type(output_type, default="")

    # new shape: treatment -> output_type -> variable
    if requested_output_type:
        output_block = treatment_block.get(requested_output_type)
        if isinstance(output_block, dict):
            rel_path = _normalize_text(output_block.get(variable))
            if rel_path:
                return {
                    "artifact_type": "timeseries",
                    "model_id": model,
                    "treatment": treatment,
                    "variable": variable,
                    "output_type": requested_output_type,
                    "series_type": requested_output_type,  # compat
                    "rel_path": rel_path,
                    "media_type": "application/json",
                    "reader": "timeseries_json_auto",
                }

    # old compatibility shape: treatment -> variable
    if requested_output_type in {"forecast_with_da", "simulation_without_da"}:
        rel_path = _normalize_text(treatment_block.get(variable))
        if rel_path:
            return {
                "artifact_type": "timeseries",
                "model_id": model,
                "treatment": treatment,
                "variable": variable,
                "output_type": requested_output_type,
                "series_type": requested_output_type,
                "rel_path": rel_path,
                "media_type": "application/json",
                "reader": "timeseries_json_auto",
            }

    return None


def find_timeseries_artifact(
    manifest: dict[str, Any],
    *,
    model: str,
    treatment: str,
    variable: str,
    output_type: str = "",
    series_type: str = "",
) -> dict[str, Any]:
    """
    Resolve one timeseries artifact from manifest.

    Lookup order:
    1. manifest["artifacts"]
    2. manifest["outputs"]["index"]

    Notes:
    - prefer output_type
    - series_type is kept for backward compatibility
    """
    requested_output_type = _normalize_requested_output_type(
        output_type or series_type,
        default="",
    )

    if not requested_output_type:
        req = manifest.get("request") or {}
        if isinstance(req, dict):
            expected = req.get("expected_output_types")
            if isinstance(expected, list) and expected:
                requested_output_type = _normalize_requested_output_type(expected[0], default="")

    artifact = _find_timeseries_artifact_from_artifacts(
        manifest,
        model=model,
        treatment=treatment,
        variable=variable,
        output_type=requested_output_type,
    )
    if artifact is not None:
        return artifact

    artifact = _find_timeseries_artifact_from_outputs_index(
        manifest,
        model=model,
        treatment=treatment,
        variable=variable,
        output_type=requested_output_type,
    )
    if artifact is not None:
        return artifact

    raise HTTPException(
        404,
        f"timeseries artifact not found for model={model}, treatment={treatment}, "
        f"variable={variable}, requested_output_type={requested_output_type or '(empty)'}",
    )


# -----------------------------------------------------------------------------
# Readers
# -----------------------------------------------------------------------------
def _load_json_file(path: Path) -> Any:
    if not path.exists():
        raise HTTPException(404, f"timeseries file not found: {path.name}")

    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as ex:
        raise HTTPException(500, f"failed to read json timeseries: {ex}")


def read_timeseries_json_simple(path: Path) -> dict[str, Any]:
    """
    Input:
    [
      {"date": "2020-01-01", "value": 1.2},
      ...
    ]
    """
    data = _load_json_file(path)

    if not isinstance(data, list):
        raise HTTPException(500, "invalid json timeseries format: expected list")

    time: list[Any] = []
    mean: list[Any] = []

    for row in data:
        if not isinstance(row, dict):
            continue
        time.append(row.get("date"))
        mean.append(row.get("value"))

    return {
        "units": "",
        "series": [
            {
                "time": time,
                "mean": mean,
                "lo": [],
                "hi": [],
            }
        ],
    }


def read_timeseries_json_standard(path: Path) -> dict[str, Any]:
    """
    Input:
    {
      "units": "...",
      "series": [
        {
          "time": [...],
          "mean": [...],
          "lo": [...],
          "hi": [...]
        }
      ]
    }
    """
    data = _load_json_file(path)

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid standard timeseries format: expected dict")

    units = data.get("units", "") or ""
    series = data.get("series", []) or []

    if not isinstance(series, list):
        raise HTTPException(500, "invalid standard timeseries format: series must be list")

    normalized_series: list[dict[str, Any]] = []
    for item in series:
        if not isinstance(item, dict):
            continue

        time = item.get("time", []) or []
        mean = item.get("mean", []) or []
        lo = item.get("lo", []) or []
        hi = item.get("hi", []) or []

        if not isinstance(time, list):
            time = []
        if not isinstance(mean, list):
            mean = []
        if not isinstance(lo, list):
            lo = []
        if not isinstance(hi, list):
            hi = []

        normalized_series.append(
            {
                "time": time,
                "mean": mean,
                "lo": lo,
                "hi": hi,
            }
        )

    return {
        "units": units,
        "series": normalized_series,
    }


def read_timeseries_json_auto(path: Path) -> dict[str, Any]:
    """
    Auto-detect JSON timeseries format.

    Supported:
    1. simple list format
    2. standard dict format
    """
    data = _load_json_file(path)

    if isinstance(data, list):
        time: list[Any] = []
        mean: list[Any] = []

        for row in data:
            if not isinstance(row, dict):
                continue
            time.append(row.get("date"))
            mean.append(row.get("value"))

        return {
            "units": "",
            "series": [
                {
                    "time": time,
                    "mean": mean,
                    "lo": [],
                    "hi": [],
                }
            ],
        }

    if isinstance(data, dict):
        units = data.get("units", "") or ""
        series = data.get("series", []) or []

        if not isinstance(series, list):
            raise HTTPException(500, "invalid standard timeseries format: series must be list")

        normalized_series: list[dict[str, Any]] = []
        for item in series:
            if not isinstance(item, dict):
                continue

            time = item.get("time", []) or []
            mean = item.get("mean", []) or []
            lo = item.get("lo", []) or []
            hi = item.get("hi", []) or []

            if not isinstance(time, list):
                time = []
            if not isinstance(mean, list):
                mean = []
            if not isinstance(lo, list):
                lo = []
            if not isinstance(hi, list):
                hi = []

            normalized_series.append(
                {
                    "time": time,
                    "mean": mean,
                    "lo": lo,
                    "hi": hi,
                }
            )

        return {
            "units": units,
            "series": normalized_series,
        }

    raise HTTPException(500, "unsupported json timeseries format")


def read_timeseries_csv(path: Path) -> dict[str, Any]:
    raise HTTPException(501, "CSV timeseries reader not implemented yet")


def read_timeseries_netcdf(path: Path) -> dict[str, Any]:
    raise HTTPException(501, "NetCDF timeseries reader not implemented yet")


def read_timeseries_from_artifact(run_dir: Path, artifact: dict[str, Any]) -> dict[str, Any]:
    rel_path = _normalize_text(artifact.get("rel_path"))
    media_type = _normalize_text(artifact.get("media_type"))
    reader_name = _normalize_text(artifact.get("reader"))

    if not rel_path:
        raise HTTPException(500, "artifact rel_path missing")

    path = run_dir / rel_path
    if not path.exists():
        raise HTTPException(404, f"artifact file not found: {rel_path}")

    if reader_name == "timeseries_json_simple":
        return read_timeseries_json_simple(path)

    if reader_name == "timeseries_json_standard":
        return read_timeseries_json_standard(path)

    if reader_name == "timeseries_json_auto":
        return read_timeseries_json_auto(path)

    if reader_name == "timeseries_csv":
        return read_timeseries_csv(path)

    if reader_name == "timeseries_netcdf":
        return read_timeseries_netcdf(path)

    if media_type == "application/json":
        return read_timeseries_json_auto(path)

    if media_type in ("text/csv", "application/csv"):
        return read_timeseries_csv(path)

    if media_type in ("application/netcdf", "application/x-netcdf"):
        return read_timeseries_netcdf(path)

    raise HTTPException(
        500,
        f"unsupported timeseries artifact media_type={media_type!r}, reader={reader_name!r}",
    )