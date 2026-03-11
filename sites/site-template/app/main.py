from __future__ import annotations

import json
import time
import subprocess
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from app.schemas import (
    RunRequest,
    ManifestResponse,
)
from app.settings import ensure_workspace, run_dir
from app.loaders import (
    build_site_meta,
    build_params_meta,
    resolve_task_command,
)
from app.timeseries import (
    load_manifest,
    find_timeseries_artifact,
    read_timeseries_from_artifact,
)
from app.parameters import (
    find_parameter_artifact,
    read_parameter_artifact,
)
from app.builders import build_and_write_manifest

app = FastAPI(title="EcoPAD Site Service")

ensure_workspace()


def _read_json_file(path: Path) -> dict[str, Any] | list[Any]:
    if not path.exists():
        raise HTTPException(404, f"{path.name} not found")

    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as ex:
        raise HTTPException(500, f"Failed to read json: {ex}")


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
        with open(rdir / "stdout.log", "w", encoding="utf-8") as stdout, \
             open(rdir / "stderr.log", "w", encoding="utf-8") as stderr:
            subprocess.run(
                cmd,
                stdout=stdout,
                stderr=stderr,
                check=True,
            )
    except subprocess.CalledProcessError:
        raise HTTPException(500, "Model execution failed")

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


@app.get("/runs/{run_id}/timeseries")
def get_timeseries(
    run_id: str,
    variable: str = Query(...),
    model: str = Query(...),
    treatment: str = Query(...),
):
    rdir = run_dir(run_id)

    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    artifact = find_timeseries_artifact(
        manifest,
        model=model,
        treatment=treatment,
        variable=variable,
    )

    data = read_timeseries_from_artifact(rdir, artifact)

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid timeseries reader output")

    return {
        "run_id": run_id,
        "variable": variable,
        "model": model,
        "treatment": treatment,
        "units": data.get("units", "") or "",
        "series": data.get("series", []) or [],
    }


@app.get("/runs/{run_id}/parameter_summary")
def get_run_parameter_summary(
    run_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
):
    rdir = run_dir(run_id)

    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    artifact = find_parameter_artifact(
        manifest,
        artifact_type="parameter_summary",
        model=model,
        treatment=treatment,
    )

    data = read_parameter_artifact(rdir, artifact)

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid parameter summary format")

    return data


@app.get("/runs/{run_id}/parameter_best")
def get_run_parameter_best(
    run_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
):
    rdir = run_dir(run_id)

    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    artifact = find_parameter_artifact(
        manifest,
        artifact_type="parameter_best",
        model=model,
        treatment=treatment,
    )

    data = read_parameter_artifact(rdir, artifact)

    if not isinstance(data, dict):
        raise HTTPException(500, "invalid parameter_best format")

    return data


@app.get("/runs/{run_id}/parameters_accepted")
def get_run_parameters_accepted(
    run_id: str,
    model: str = Query(..., description="Model ID"),
    treatment: str = Query(..., description="Treatment ID"),
):
    rdir = run_dir(run_id)

    if not rdir.exists():
        raise HTTPException(404, "run not found")

    manifest = load_manifest(rdir)

    artifact = find_parameter_artifact(
        manifest,
        artifact_type="parameters_accepted",
        model=model,
        treatment=treatment,
    )

    data = read_parameter_artifact(rdir, artifact)

    if not isinstance(data, list):
        raise HTTPException(500, "invalid parameters_accepted format")

    return {
        "run_id": run_id,
        "model": model,
        "treatment": treatment,
        "rows": data,
    }