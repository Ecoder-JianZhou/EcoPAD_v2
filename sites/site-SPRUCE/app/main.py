from __future__ import annotations

import csv
import io
import json
import mimetypes
import shutil
import subprocess
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

from app.builders import build_and_write_manifest
from app.loaders import (
    build_params_meta,
    build_site_meta,
    load_model_config,
    resolve_task_command,
)
from app.parameters import (
    find_parameter_artifact,
    read_parameter_artifact,
)
from app.schemas import (
    ManifestResponse,
    RunRequest,
)
from app.settings import ensure_workspace, run_dir
from app.timeseries import (
    find_timeseries_artifact,
    load_manifest,
    read_timeseries_from_artifact,
)

app = FastAPI(title="EcoPAD Site Service")

ensure_workspace()


SUPPORTED_OUTPUT_TYPES = (
    "simulation_without_da",
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
    "auto_forecast_with_da",
    "auto_forecast_without_da",
)

LEGACY_OUTPUT_ALIASES = {
    "simulate": "simulation_without_da",
    "simulation_with_da": "simulation_with_da",
    "forecast_with_da": "forecast_with_da",
    "forecast_without_da": "forecast_without_da",
    "with_da": "forecast_with_da",
    "without_da": "forecast_without_da",
}


# ---------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------
def _read_json_file(path: Path) -> dict[str, Any] | list[Any]:
    if not path.exists():
        raise HTTPException(404, f"{path.name} not found")

    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as ex:
        raise HTTPException(500, f"Failed to read json: {ex}")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _normalize_task_type(value: Any) -> str:
    """
    Normalize task type to canonical internal names.
    """
    text = _normalize_text(value).lower()

    mapping = {
        "simulate": "simulation_without_da",
        "simulation_without_da": "simulation_without_da",
        "simulation without da": "simulation_without_da",
        "simulation_with_da": "simulation_with_da",
        "simulation with da": "simulation_with_da",
        "forecast_with_da": "forecast_with_da",
        "forecast with da": "forecast_with_da",
        "forecast_without_da": "forecast_without_da",
        "forecast without da": "forecast_without_da",
        "auto_forecast": "auto_forecast",
        "auto forecast": "auto_forecast",
    }

    return mapping.get(text, text)


def _normalize_output_type(value: Any, default: str = "") -> str:
    """
    Normalize output type.

    Supports:
    - canonical output_type
    - legacy aliases
    """
    text = _normalize_text(value).lower()
    if not text:
        return default

    out = LEGACY_OUTPUT_ALIASES.get(text, text)
    if out not in SUPPORTED_OUTPUT_TYPES:
        return default
    return out


def _resolve_expected_output_types_for_run(
    *,
    task_type: str,
    payload: dict[str, Any],
) -> list[str]:
    """
    Resolve expected output types for one run from request info.
    """
    task = _normalize_task_type(task_type)
    payload = payload if isinstance(payload, dict) else {}

    if task == "simulation_without_da":
        return ["simulation_without_da"]

    if task == "simulation_with_da":
        return ["simulation_with_da"]

    if task == "forecast_with_da":
        return ["forecast_with_da"]

    if task == "forecast_without_da":
        return ["forecast_without_da"]

    if task == "auto_forecast":
        requested_mode = _normalize_output_type(payload.get("series_type"), default="")
        if requested_mode == "forecast_with_da":
            return ["auto_forecast_with_da"]
        if requested_mode == "forecast_without_da":
            return ["auto_forecast_without_da"]

        enable_with_da = _normalize_bool(payload.get("auto_forecast_with_da"), True)
        enable_without_da = _normalize_bool(
            payload.get("auto_forecast_without_da"),
            _normalize_bool(payload.get("include_without_da"), True),
        )

        out: list[str] = []
        if enable_with_da:
            out.append("auto_forecast_with_da")
        if enable_without_da:
            out.append("auto_forecast_without_da")

        if not out:
            out = ["auto_forecast_with_da"]

        return out

    return []


def _candidate_output_types(
    *,
    requested_output_type: str,
    manifest: dict[str, Any] | None = None,
) -> list[str]:
    """
    Build candidate output types in priority order.

    Priority:
    1. requested output_type
    2. manifest.request.expected_output_types
    3. derived from task_type + payload
    """
    out: list[str] = []
    seen: set[str] = set()

    def add(v: Any) -> None:
        text = _normalize_output_type(v, default="")
        if not text or text in seen:
            return
        seen.add(text)
        out.append(text)

    add(requested_output_type)

    if isinstance(manifest, dict):
        req = manifest.get("request")
        if isinstance(req, dict):
            expected = req.get("expected_output_types")
            if isinstance(expected, list):
                for item in expected:
                    add(item)

            task = req.get("task")
            payload = req.get("payload")
            if isinstance(payload, dict):
                for item in _resolve_expected_output_types_for_run(
                    task_type=_normalize_task_type(task),
                    payload=payload,
                ):
                    add(item)

    return out


def _find_observation_config(model_id: str, variable: str) -> dict[str, Any] | None:
    model_cfg = load_model_config(model_id)
    observations = model_cfg.get("observations") or []

    if not isinstance(observations, list):
        return None

    variable_text = _normalize_text(variable)

    for item in observations:
        if not isinstance(item, dict):
            continue
        v = _normalize_text(item.get("variable") or item.get("id"))
        if v == variable_text:
            return item

    return None


def _resolve_observation_path(model_id: str, variable: str) -> Path:
    cfg = _find_observation_config(model_id, variable)
    if not cfg:
        raise HTTPException(
            404,
            f"observation config not found for model={model_id}, variable={variable}",
        )

    raw_path = _normalize_text(cfg.get("path"))
    if not raw_path:
        raise HTTPException(
            404,
            f"observation path missing for model={model_id}, variable={variable}",
        )

    path = Path("/app") / raw_path
    if not path.exists():
        raise HTTPException(404, f"observation file not found: {raw_path}")

    return path


def _row_to_iso_time(year: Any, doy: Any, hour: Any) -> str:
    try:
        y = int(year)
        d = int(doy)
        h = int(hour)
    except Exception:
        return ""

    try:
        dt = datetime(y, 1, 1) + timedelta(days=d - 1, hours=h)
        return dt.isoformat(timespec="seconds")
    except Exception:
        return ""


def _read_observation_csv(path: Path) -> dict[str, Any]:
    time_list: list[str] = []
    value_list: list[float | None] = []
    std_list: list[float | None] = []

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    continue

                t = _row_to_iso_time(
                    row.get("year"),
                    row.get("doy"),
                    row.get("hour"),
                )
                if not t:
                    continue

                try:
                    value = (
                        float(row.get("value"))
                        if row.get("value") not in (None, "")
                        else None
                    )
                except Exception:
                    value = None

                try:
                    std = (
                        float(row.get("std"))
                        if row.get("std") not in (None, "")
                        else None
                    )
                except Exception:
                    std = None

                time_list.append(t)
                value_list.append(value)
                std_list.append(std)

    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(500, f"failed to read observation csv: {ex}")

    return {
        "time": time_list,
        "value": value_list,
        "std": std_list,
    }


def _read_log_tail(path: Path, n: int = 20) -> str:
    if not path.exists():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return "\n".join(lines[-n:])
    except Exception:
        return ""


def _guess_media_type(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def _load_artifacts_from_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = manifest.get("artifacts") or []
    return [x for x in artifacts if isinstance(x, dict)]


def _artifact_output_type(item: dict[str, Any]) -> str:
    metadata = item.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    return _normalize_text(
        item.get("output_type")
        or item.get("series_type")
        or metadata.get("output_type")
        or metadata.get("series_type")
    )


def _artifact_to_download_item(rdir: Path, item: dict[str, Any], idx: int) -> dict[str, Any]:
    rel_path = _normalize_text(item.get("rel_path") or item.get("path"))
    full_path = (rdir / rel_path).resolve() if rel_path else None

    exists = False
    size = None

    if full_path is not None:
        try:
            full_path.relative_to(rdir.resolve())
            exists = full_path.exists() and full_path.is_file()
            if exists:
                size = full_path.stat().st_size
        except Exception:
            exists = False
            size = None

    return {
        "id": idx + 1,
        "artifact_type": _normalize_text(item.get("artifact_type") or item.get("type")),
        "model_id": _normalize_text(item.get("model_id")),
        "treatment": _normalize_text(item.get("treatment")),
        "variable": _normalize_text(item.get("variable")),
        "output_type": _artifact_output_type(item),
        "series_type": _artifact_output_type(item),
        "rel_path": rel_path,
        "filename": Path(rel_path).name if rel_path else "",
        "media_type": _normalize_text(item.get("media_type")) or (
            _guess_media_type(full_path) if full_path else "application/octet-stream"
        ),
        "exists": exists,
        "size": size,
        "metadata": item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
    }


def _resolve_artifact_path(
    *,
    rdir: Path,
    manifest: dict[str, Any],
    rel_path: str = "",
    artifact_id: int | None = None,
) -> tuple[Path, str]:
    """
    Resolve one concrete downloadable file inside a run directory.
    """
    artifacts = _load_artifacts_from_manifest(manifest)
    selected: dict[str, Any] | None = None

    if artifact_id is not None:
        idx = int(artifact_id) - 1
        if idx < 0 or idx >= len(artifacts):
            raise HTTPException(404, f"Artifact not found: {artifact_id}")
        selected = artifacts[idx]

    elif rel_path:
        clean_rel = _normalize_text(rel_path)
        for item in artifacts:
            candidate = _normalize_text(item.get("rel_path") or item.get("path"))
            if candidate == clean_rel:
                selected = item
                break

        if selected is None:
            full = (rdir / clean_rel).resolve()
            try:
                full.relative_to(rdir.resolve())
            except Exception:
                raise HTTPException(400, "Invalid rel_path.")
            if not full.exists() or not full.is_file():
                raise HTTPException(404, f"File not found: {clean_rel}")
            return full, clean_rel

    else:
        raise HTTPException(400, "artifact_id or rel_path is required.")

    selected_rel = _normalize_text(selected.get("rel_path") or selected.get("path")) if selected else ""
    if not selected_rel:
        raise HTTPException(404, "Artifact path is empty.")

    full_path = (rdir / selected_rel).resolve()
    try:
        full_path.relative_to(rdir.resolve())
    except Exception:
        raise HTTPException(400, "Resolved artifact path is invalid.")

    if not full_path.exists() or not full_path.is_file():
        raise HTTPException(404, f"Artifact file not found: {selected_rel}")

    return full_path, selected_rel


def _build_run_bundle_bytes(rdir: Path, manifest: dict[str, Any], run_id: str) -> tuple[bytes, str]:
    """
    Build an in-memory zip for one run.
    """
    buffer = io.BytesIO()
    used_names: set[str] = set()

    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for base_name in ("manifest.json", "request.json", "stdout.log", "stderr.log", "executor_summary.json"):
            p = rdir / base_name
            if p.exists() and p.is_file():
                zf.write(p, base_name)
                used_names.add(base_name)

        for item in _load_artifacts_from_manifest(manifest):
            rel_path = _normalize_text(item.get("rel_path") or item.get("path"))
            if not rel_path:
                continue

            full_path = (rdir / rel_path).resolve()
            try:
                full_path.relative_to(rdir.resolve())
            except Exception:
                continue

            if not full_path.exists() or not full_path.is_file():
                continue

            arcname = rel_path.replace("\\", "/")
            if arcname in used_names:
                continue

            zf.write(full_path, arcname)
            used_names.add(arcname)

    buffer.seek(0)
    return buffer.getvalue(), f"run-{run_id}.zip"


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@app.get("/meta")
def meta():
    return build_site_meta()


@app.get("/params/meta")
def params_meta(model: str = Query(...)):
    meta = build_site_meta()
    site_id = meta["site_id"]
    return build_params_meta(model, site_id)


@app.post("/run")
def run_model(req: RunRequest):
    rdir = run_dir(req.run_id)

    if rdir.exists():
        raise HTTPException(400, "run_id already exists")

    rdir.mkdir(parents=True)

    with open(rdir / "request.json", "w", encoding="utf-8") as f:
        json.dump(req.dict(), f, indent=2, ensure_ascii=False)

    try:
        cmd = resolve_task_command(
            model_id=req.model_id,
            task_type=req.task_type,
            run_dir=str(rdir),
        )
    except Exception as ex:
        raise HTTPException(400, f"Failed to resolve task command: {ex}")

    try:
        with open(rdir / "stdout.log", "w", encoding="utf-8") as stdout, open(
            rdir / "stderr.log", "w", encoding="utf-8"
        ) as stderr:
            subprocess.run(
                cmd,
                stdout=stdout,
                stderr=stderr,
                check=True,
            )
    except subprocess.CalledProcessError as ex:
        stderr_tail = _read_log_tail(rdir / "stderr.log")
        detail = f"Model execution failed (exit code {ex.returncode})"
        if stderr_tail:
            detail += f"\n{stderr_tail}"
        raise HTTPException(500, detail)

    try:
        manifest = build_and_write_manifest(rdir)
    except Exception as ex:
        raise HTTPException(500, f"failed to build manifest: {ex}")

    return {
        "status": "ok",
        "run_id": req.run_id,
        "manifest_written": True,
        "artifacts": len(manifest.get("artifacts", [])),
    }


@app.get("/runs/{run_id}/manifest", response_model=ManifestResponse)
def get_manifest(run_id: str):
    rdir = run_dir(run_id)
    path = rdir / "manifest.json"

    if not path.exists():
        raise HTTPException(404, "manifest not found")

    last_error = None

    for _ in range(5):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except OSError as ex:
            last_error = ex
            time.sleep(0.05)
        except Exception as ex:
            raise HTTPException(500, f"failed to read manifest: {ex}")

    raise HTTPException(500, f"failed to read manifest after retries: {last_error}")


@app.get("/runs/{run_id}/artifacts")
def get_run_artifacts(run_id: str):
    """
    Return normalized artifact list for download UI.
    """
    rdir = run_dir(run_id)
    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)
    artifacts = _load_artifacts_from_manifest(manifest)

    items = [
        _artifact_to_download_item(rdir, item, idx)
        for idx, item in enumerate(artifacts)
    ]

    return {
        "run_id": run_id,
        "artifacts": items,
    }


@app.get("/runs/{run_id}/download")
def download_run_artifact(
    run_id: str,
    rel_path: str = "",
    artifact_id: str = "",
    bundle: str = "0",
):
    """
    Download one artifact file or a bundled zip for one run.

    Query parameters are parsed manually to avoid 422 caused by empty
    query values such as artifact_id=.
    """
    rdir = run_dir(run_id)
    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    bundle_flag = str(bundle or "").strip().lower() in {"1", "true", "yes", "y", "on"}

    artifact_id_int: int | None = None
    artifact_id_text = str(artifact_id or "").strip()
    if artifact_id_text:
        try:
            artifact_id_int = int(artifact_id_text)
        except Exception:
            raise HTTPException(400, "artifact_id must be an integer.")

    rel_path_text = str(rel_path or "").strip()

    if bundle_flag:
        content, filename = _build_run_bundle_bytes(rdir, manifest, run_id)
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
        return StreamingResponse(
            io.BytesIO(content),
            media_type="application/zip",
            headers=headers,
        )

    full_path, selected_rel = _resolve_artifact_path(
        rdir=rdir,
        manifest=manifest,
        rel_path=rel_path_text,
        artifact_id=artifact_id_int,
    )

    media_type = _guess_media_type(full_path)
    return FileResponse(
        path=full_path,
        media_type=media_type,
        filename=Path(selected_rel).name,
    )


@app.delete("/runs/{run_id}")
def delete_run_workspace(run_id: str):
    """
    Delete one site-side run workspace directory.
    """
    rdir = run_dir(run_id)
    if not rdir.exists():
        return {
            "ok": True,
            "run_id": run_id,
            "removed": False,
            "detail": "Run directory does not exist.",
        }

    try:
        shutil.rmtree(rdir)
    except Exception as ex:
        raise HTTPException(500, f"failed to remove run directory: {ex}")

    return {
        "ok": True,
        "run_id": run_id,
        "removed": True,
    }


@app.get("/runs/{run_id}/timeseries")
def get_timeseries(
    run_id: str,
    variable: str = Query(...),
    model: str = Query(...),
    treatment: str = Query(...),
    output_type: str = Query(default=""),
    series_type: str = Query(default=""),
):
    rdir = run_dir(run_id)

    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    requested_output_type = _normalize_output_type(
        output_type or series_type,
        default="",
    )

    artifact = None
    resolved_output_type = ""

    for candidate in _candidate_output_types(
        requested_output_type=requested_output_type,
        manifest=manifest,
    ):
        try:
            artifact = find_timeseries_artifact(
                manifest,
                model=model,
                treatment=treatment,
                variable=variable,
                series_type=candidate,
            )
            resolved_output_type = candidate
            break
        except HTTPException:
            continue

    if artifact is None:
        raise HTTPException(
            404,
            f"timeseries artifact not found for "
            f"model={model}, treatment={treatment}, variable={variable}, "
            f"output_type={requested_output_type or '(auto)'}",
        )

    data = read_timeseries_from_artifact(rdir, artifact)

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid timeseries reader output")

    return {
        "run_id": run_id,
        "variable": variable,
        "model": model,
        "treatment": treatment,
        "output_type": resolved_output_type,
        "series_type": resolved_output_type,
        "units": data.get("units", "") or "",
        "series": data.get("series", []) or [],
    }


@app.get("/obs")
def get_observation(
    variable: str = Query(...),
    treatment: str = Query(""),
    model: str = Query(...),
):
    path = _resolve_observation_path(model, variable)
    data = _read_observation_csv(path)

    return {
        "model": model,
        "variable": variable,
        "treatment": treatment,
        "time": data.get("time") or [],
        "value": data.get("value") or [],
        "std": data.get("std") or [],
    }


@app.get("/runs/{run_id}/parameter_summary")
def get_run_parameter_summary(
    run_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
    output_type: str = Query(default="", description="Output type"),
    series_type: str = Query(default="", description="Backward-compatible alias"),
):
    rdir = run_dir(run_id)

    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    requested_output_type = _normalize_output_type(output_type or series_type, default="")

    artifact = None
    resolved_output_type = ""

    for candidate in _candidate_output_types(
        requested_output_type=requested_output_type,
        manifest=manifest,
    ):
        try:
            artifact = find_parameter_artifact(
                manifest,
                artifact_type="parameter_summary",
                model=model,
                treatment=treatment,
                output_type=candidate,
            )
            resolved_output_type = candidate
            break
        except HTTPException:
            continue

    if artifact is None:
        raise HTTPException(
            404,
            f"parameter_summary not found for model={model}, treatment={treatment}, "
            f"output_type={requested_output_type or '(auto)'}",
        )

    data = read_parameter_artifact(rdir, artifact)

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid parameter summary format")

    if "output_type" not in data:
        data["output_type"] = resolved_output_type
    if "series_type" not in data:
        data["series_type"] = resolved_output_type

    return data


@app.get("/runs/{run_id}/parameter_best")
def get_run_parameter_best(
    run_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
    output_type: str = Query(default="", description="Output type"),
    series_type: str = Query(default="", description="Backward-compatible alias"),
):
    rdir = run_dir(run_id)

    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    requested_output_type = _normalize_output_type(output_type or series_type, default="")

    artifact = None
    resolved_output_type = ""

    for candidate in _candidate_output_types(
        requested_output_type=requested_output_type,
        manifest=manifest,
    ):
        try:
            artifact = find_parameter_artifact(
                manifest,
                artifact_type="parameter_best",
                model=model,
                treatment=treatment,
                output_type=candidate,
            )
            resolved_output_type = candidate
            break
        except HTTPException:
            continue

    if artifact is None:
        raise HTTPException(
            404,
            f"parameter_best not found for model={model}, treatment={treatment}, "
            f"output_type={requested_output_type or '(auto)'}",
        )

    data = read_parameter_artifact(rdir, artifact)

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid parameter_best format")

    if "output_type" not in data:
        data["output_type"] = resolved_output_type
    if "series_type" not in data:
        data["series_type"] = resolved_output_type

    return data


@app.get("/runs/{run_id}/parameters_accepted")
def get_run_parameters_accepted(
    run_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
    output_type: str = Query(default="", description="Output type"),
    series_type: str = Query(default="", description="Backward-compatible alias"),
):
    rdir = run_dir(run_id)

    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    requested_output_type = _normalize_output_type(output_type or series_type, default="")

    artifact = None
    resolved_output_type = ""

    for candidate in _candidate_output_types(
        requested_output_type=requested_output_type,
        manifest=manifest,
    ):
        try:
            artifact = find_parameter_artifact(
                manifest,
                artifact_type="parameters_accepted",
                model=model,
                treatment=treatment,
                output_type=candidate,
            )
            resolved_output_type = candidate
            break
        except HTTPException:
            continue

    if artifact is None:
        raise HTTPException(
            404,
            f"parameters_accepted not found for model={model}, treatment={treatment}, "
            f"output_type={requested_output_type or '(auto)'}",
        )

    data = read_parameter_artifact(rdir, artifact)

    if not isinstance(data, list):
        raise HTTPException(500, "invalid parameters_accepted format")

    return {
        "run_id": run_id,
        "model": model,
        "treatment": treatment,
        "output_type": resolved_output_type,
        "series_type": resolved_output_type,
        "rows": data,
    }