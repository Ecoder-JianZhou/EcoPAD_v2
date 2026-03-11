from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException


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


def find_timeseries_artifact(
    manifest: dict[str, Any],
    *,
    model: str,
    treatment: str,
    variable: str,
) -> dict[str, Any]:
    artifacts = manifest.get("artifacts") or []
    if not isinstance(artifacts, list):
        artifacts = []

    for item in artifacts:
        if not isinstance(item, dict):
            continue

        if str(item.get("artifact_type") or "") != "timeseries":
            continue
        if str(item.get("model_id") or "") != model:
            continue
        if str(item.get("treatment") or "") != treatment:
            continue
        if str(item.get("variable") or "") != variable:
            continue

        return item

    raise HTTPException(
        404,
        f"timeseries artifact not found for model={model}, treatment={treatment}, variable={variable}",
    )


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
    Input format:
    [
      {"date": "2020-01-01", "value": 1.2},
      ...
    ]

    Output format:
    {
      "units": "",
      "series": [
        {
          "time": [...],
          "mean": [...],
          "lo": [],
          "hi": []
        }
      ]
    }
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
    Input format:
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
    1. simple list format:
       [
         {"date": "...", "value": ...}
       ]

    2. standard dict format:
       {
         "units": "...",
         "series": [...]
       }
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
    """
    Placeholder for future CSV support.
    Expected columns could be:
    time,mean[,lo,hi]
    """
    raise HTTPException(501, "CSV timeseries reader not implemented yet")


def read_timeseries_netcdf(path: Path) -> dict[str, Any]:
    """
    Placeholder for future NetCDF support.
    """
    raise HTTPException(501, "NetCDF timeseries reader not implemented yet")


def read_timeseries_from_artifact(run_dir: Path, artifact: dict[str, Any]) -> dict[str, Any]:
    rel_path = str(artifact.get("rel_path") or "").strip()
    media_type = str(artifact.get("media_type") or "").strip()
    reader_name = str(artifact.get("reader") or "").strip()

    if not rel_path:
        raise HTTPException(500, "artifact rel_path missing")

    path = run_dir / rel_path
    if not path.exists():
        raise HTTPException(404, f"artifact file not found: {rel_path}")

    # reader explicitly declared in manifest wins
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

    # fallback by media_type
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