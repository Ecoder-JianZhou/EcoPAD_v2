from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.loaders import load_model_config, load_model_variables, load_run_request


def _normalize_rel(path: Path, root: Path) -> str:
    return str(path.relative_to(root)).replace("\\", "/")


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8")
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _summary_metadata(summary_obj: dict[str, Any], *, model_id: str, treatment: str) -> dict[str, Any]:
    out: dict[str, Any] = {"model_id": model_id, "treatment": treatment}
    params = summary_obj.get("summary", {}).get("parameters") if isinstance(summary_obj.get("summary"), dict) else None
    if not isinstance(params, list):
        params = summary_obj.get("parameters")
    if not isinstance(params, list):
        return out
    param_map: dict[str, Any] = {}
    for item in params:
        if not isinstance(item, dict):
            continue
        pid = str(item.get("id") or "").strip()
        if not pid:
            continue
        param_map[pid] = {
            "value": item.get("optimized", item.get("value", item.get("mean", item.get("map")))),
            "accepted_min": item.get("accepted_min", item.get("minimum", item.get("p05", item.get("q05")))),
            "accepted_max": item.get("accepted_max", item.get("maximum", item.get("p95", item.get("q95")))),
            "mean": item.get("mean"),
            "median": item.get("median"),
            "sd": item.get("sd"),
            "map": item.get("map"),
            "unit": item.get("unit"),
            "name": item.get("name"),
        }
    if param_map:
        out["parameters"] = param_map
    return out


def _discover_timeseries_artifacts(rdir: Path, *, model_id: str, treatments: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    outputs_index: dict[str, Any] = {model_id: {}}
    outputs_root = rdir / "outputs" / model_id
    if not outputs_root.exists():
        return artifacts, outputs_index
    variables_cfg = load_model_variables(model_id)
    variable_specs: list[tuple[str, str]] = []
    for item in variables_cfg:
        variable = str(item.get("name") or item.get("id") or "").strip()
        output_file = str(item.get("output_file") or f"{variable}.json").strip()
        if not variable or not output_file:
            continue
        variable_specs.append((variable, output_file))
    for treatment in treatments:
        tdir = outputs_root / treatment
        if not tdir.exists():
            continue
        outputs_index[model_id][treatment] = {}
        for variable, output_file in variable_specs:
            path = tdir / output_file
            if not path.exists():
                continue
            rel_path = _normalize_rel(path, rdir)
            artifacts.append({
                "artifact_type": "timeseries",
                "model_id": model_id,
                "treatment": treatment,
                "variable": variable,
                "rel_path": rel_path,
                "media_type": "application/json",
                "reader": "timeseries_json_standard",
                "metadata": {},
            })
            outputs_index[model_id][treatment][variable] = rel_path
    return artifacts, outputs_index


def _discover_parameter_artifacts(rdir: Path, *, model_id: str, treatments: list[str]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    model_cfg = load_model_config(model_id)
    param_outputs = model_cfg.get("parameter_outputs", {}) or {}
    summary_name = str(param_outputs.get("summary") or "summary.json")
    accepted_name = str(param_outputs.get("accepted") or "parameters_accepted.csv")
    best_name = str(param_outputs.get("best") or "best.json")
    for treatment in treatments:
        tdir = rdir / "outputs" / model_id / treatment
        if not tdir.exists():
            continue
        summary_path = tdir / summary_name
        if summary_path.exists():
            summary_obj = _load_json_if_exists(summary_path)
            artifacts.append({
                "artifact_type": "parameter_summary",
                "model_id": model_id,
                "treatment": treatment,
                "variable": "",
                "rel_path": _normalize_rel(summary_path, rdir),
                "media_type": "application/json",
                "metadata": _summary_metadata(summary_obj, model_id=model_id, treatment=treatment),
            })
        accepted_path = tdir / accepted_name
        if accepted_path.exists():
            artifacts.append({
                "artifact_type": "parameters_accepted",
                "model_id": model_id,
                "treatment": treatment,
                "variable": "",
                "rel_path": _normalize_rel(accepted_path, rdir),
                "media_type": "text/csv",
                "metadata": {"model_id": model_id, "treatment": treatment},
            })
        best_path = tdir / best_name
        if best_path.exists():
            artifacts.append({
                "artifact_type": "parameter_best",
                "model_id": model_id,
                "treatment": treatment,
                "variable": "",
                "rel_path": _normalize_rel(best_path, rdir),
                "media_type": "application/json",
                "metadata": {"model_id": model_id, "treatment": treatment},
            })
    return artifacts


def _build_forecast_registry(*, model_id: str, artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    model_cfg = load_model_config(model_id)
    raw_default_publish = model_cfg.get("default_publish_variables") or []
    default_publish = {str(v).strip().lower() for v in raw_default_publish if str(v).strip()}
    publish_all = len(default_publish) == 0
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for a in artifacts:
        if a.get("artifact_type") != "timeseries":
            continue
        variable = str(a.get("variable") or "").strip()
        treatment = str(a.get("treatment") or "").strip()
        rel_path = str(a.get("rel_path") or "").strip()
        media_type = str(a.get("media_type") or "application/json").strip()
        if not variable or not treatment or not rel_path:
            continue
        key = (model_id, treatment, variable)
        should_publish = publish_all or (variable.lower() in default_publish)
        if should_publish and key not in seen:
            seen.add(key)
            items.append({
                "model_id": model_id,
                "treatment": treatment,
                "variable": variable,
                "data_path": rel_path,
                "source_ref": {"rel_path": rel_path, "media_type": media_type},
                "is_published": 1,
            })
    return items


def build_manifest(rdir: Path) -> dict[str, Any]:
    req = load_run_request(rdir)
    run_id = str(req.get("run_id") or rdir.name)
    scheduled_task_id = req.get("scheduled_task_id")
    site_id = str(req.get("site_id") or "")
    model_id = str(req.get("model_id") or "")
    task_type = str(req.get("task_type") or "")
    trigger_type = str(req.get("trigger_type") or "manual")
    payload = req.get("payload") or {}
    treatments = payload.get("treatments") or []
    if not isinstance(treatments, list):
        treatments = []
    ts_artifacts, outputs_index = _discover_timeseries_artifacts(rdir, model_id=model_id, treatments=treatments)
    param_artifacts = _discover_parameter_artifacts(rdir, model_id=model_id, treatments=treatments)
    artifacts = ts_artifacts + param_artifacts
    forecast_registry = _build_forecast_registry(model_id=model_id, artifacts=artifacts)
    manifest = {
        "run_id": run_id,
        "scheduled_task_id": scheduled_task_id,
        "site_id": site_id,
        "model_id": model_id,
        "task_type": task_type,
        "trigger_type": trigger_type,
        "request": {
            "models": [model_id],
            "treatments": treatments,
            "task": task_type,
            "payload": payload,
        },
        "execution": {"status": "done"},
        "outputs": {"index": outputs_index},
        "artifacts": artifacts,
        "forecast_registry": forecast_registry,
    }
    return manifest


def build_and_write_manifest(rdir: Path) -> dict[str, Any]:
    manifest = build_manifest(rdir)
    path = rdir / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
