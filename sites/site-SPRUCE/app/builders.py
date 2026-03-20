from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.loaders import load_model_config, load_model_variables, load_run_request


# -----------------------------------------------------------------------------
# Canonical output types written by Site executors
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------
def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


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


def _normalize_task_type(task_type: str) -> str:
    """
    Normalize task type to canonical internal names.
    """
    t = _normalize_text(task_type).lower()

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

    return mapping.get(t, t)


def _normalize_output_type(value: Any, default: str = "") -> str:
    text = _normalize_text(value).lower()
    if not text:
        return default

    out = LEGACY_OUTPUT_ALIASES.get(text, text)
    if out not in SUPPORTED_OUTPUT_TYPES:
        return default
    return out


def _normalize_requested_mode(value: Any) -> str:
    """
    Compatibility helper for older payload.series_type style.
    """
    return _normalize_output_type(value, default="")


def _resolve_expected_output_types(
    *,
    task_type: str,
    payload: dict[str, Any],
) -> list[str]:
    """
    Resolve which output directories should be scanned for THIS run.
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
        requested_mode = _normalize_requested_mode(payload.get("series_type"))

        # backward compatibility
        if requested_mode == "forecast_with_da":
            return ["auto_forecast_with_da"]

        if requested_mode == "forecast_without_da":
            return ["auto_forecast_without_da"]

        enable_with_da = bool(payload.get("auto_forecast_with_da", True))
        enable_without_da = bool(
            payload.get(
                "auto_forecast_without_da",
                payload.get("include_without_da", True),
            )
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


def _summary_metadata(
    summary_obj: dict[str, Any],
    *,
    model_id: str,
    treatment: str,
    output_type: str = "",
) -> dict[str, Any]:
    """
    Flatten parameter summary into artifact metadata for fast lookup in Runner.
    """
    out: dict[str, Any] = {
        "model_id": model_id,
        "treatment": treatment,
    }

    if output_type:
        out["output_type"] = output_type
        out["series_type"] = output_type

    params = (
        summary_obj.get("summary", {}).get("parameters")
        if isinstance(summary_obj.get("summary"), dict)
        else None
    )
    if not isinstance(params, list):
        params = summary_obj.get("parameters")

    # Accept old dict-style summary too
    if isinstance(params, dict):
        param_map = {}
        for pid, item in params.items():
            if not isinstance(item, dict):
                continue
            pid_text = str(pid or "").strip()
            if not pid_text:
                continue
            param_map[pid_text] = {
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
            "accepted_min": item.get(
                "accepted_min",
                item.get("minimum", item.get("p05", item.get("q05"))),
            ),
            "accepted_max": item.get(
                "accepted_max",
                item.get("maximum", item.get("p95", item.get("q95"))),
            ),
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


def _load_observation_map(model_id: str) -> dict[str, str]:
    """
    Build variable -> obs_path mapping from model config.
    """
    model_cfg = load_model_config(model_id)
    out: dict[str, str] = {}

    raw_obs = model_cfg.get("observations")
    if isinstance(raw_obs, list):
        for item in raw_obs:
            if not isinstance(item, dict):
                continue
            variable = str(item.get("variable") or item.get("id") or "").strip()
            path = str(item.get("path") or "").strip()
            if variable and path:
                out[variable] = path

    raw_map = model_cfg.get("observation_map")
    if isinstance(raw_map, dict):
        for key, value in raw_map.items():
            variable = str(key or "").strip()
            path = str(value or "").strip()
            if variable and path:
                out[variable] = path

    return out


def _add_unique_artifact(artifacts: list[dict[str, Any]], item: dict[str, Any]) -> None:
    """
    Prevent duplicate artifact rows.
    """
    key = (
        _normalize_text(item.get("artifact_type")),
        _normalize_text(item.get("model_id")),
        _normalize_text(item.get("treatment")),
        _normalize_text(item.get("variable")),
        _normalize_text(item.get("output_type") or item.get("series_type")),
        _normalize_text(item.get("rel_path")),
    )

    for old in artifacts:
        old_key = (
            _normalize_text(old.get("artifact_type")),
            _normalize_text(old.get("model_id")),
            _normalize_text(old.get("treatment")),
            _normalize_text(old.get("variable")),
            _normalize_text(old.get("output_type") or old.get("series_type")),
            _normalize_text(old.get("rel_path")),
        )
        if old_key == key:
            return

    artifacts.append(item)


# -----------------------------------------------------------------------------
# Artifact discovery
# -----------------------------------------------------------------------------
def _discover_timeseries_artifacts(
    rdir: Path,
    *,
    model_id: str,
    treatments: list[str],
    expected_output_types: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Discover real timeseries artifacts.

    Expected directory structure:
      outputs/<model_id>/<treatment>/<output_type>/<file>
    """
    artifacts: list[dict[str, Any]] = []
    outputs_index: dict[str, Any] = {model_id: {}}

    outputs_root = rdir / "outputs" / model_id
    if not outputs_root.exists():
        return artifacts, outputs_index

    variables_cfg = load_model_variables(model_id)
    variable_specs: list[tuple[str, str]] = []

    for item in variables_cfg:
        variable = str(item.get("name") or item.get("id") or "").strip()
        output_file = str(
            item.get("filename")
            or item.get("output_file")
            or f"{variable}.json"
        ).strip()

        if not variable or not output_file:
            continue

        variable_specs.append((variable, output_file))

    for treatment in treatments:
        treatment_text = _normalize_text(treatment)
        if not treatment_text:
            continue

        tdir = outputs_root / treatment_text
        if not tdir.exists():
            continue

        treatment_index: dict[str, Any] = {}

        for output_type in expected_output_types:
            output_type_text = _normalize_text(output_type)
            if output_type_text not in SUPPORTED_OUTPUT_TYPES:
                continue

            sdir = tdir / output_type_text
            if not sdir.exists():
                continue

            output_index: dict[str, str] = {}

            for variable, output_file in variable_specs:
                path = sdir / output_file
                if not path.exists():
                    continue

                rel_path = _normalize_rel(path, rdir)

                _add_unique_artifact(
                    artifacts,
                    {
                        "artifact_type": "timeseries",
                        "model_id": model_id,
                        "treatment": treatment_text,
                        "output_type": output_type_text,
                        "series_type": output_type_text,
                        "variable": variable,
                        "rel_path": rel_path,
                        "media_type": "application/json",
                        "reader": "timeseries_json_auto",
                        "metadata": {
                            "model_id": model_id,
                            "treatment": treatment_text,
                            "output_type": output_type_text,
                            "series_type": output_type_text,
                            "variable": variable,
                        },
                    },
                )

                output_index[variable] = rel_path

            if output_index:
                treatment_index[output_type_text] = output_index

        if treatment_index:
            outputs_index[model_id][treatment_text] = treatment_index

    return artifacts, outputs_index


def _discover_parameter_artifacts(
    rdir: Path,
    *,
    model_id: str,
    treatments: list[str],
    expected_output_types: list[str],
) -> list[dict[str, Any]]:
    """
    Discover parameter artifacts.

    Supports:
    1. Legacy single-file layout
    2. Branch-specific layout for auto_forecast / forecast branches
    """
    artifacts: list[dict[str, Any]] = []
    model_cfg = load_model_config(model_id)
    param_outputs = model_cfg.get("parameter_outputs", {}) or {}

    summary_name = str(param_outputs.get("summary") or "summary.json")
    accepted_name = str(param_outputs.get("accepted") or "parameters_accepted.csv")
    best_name = str(param_outputs.get("best") or "best.json")

    summary_stem = Path(summary_name).stem
    summary_suffix = Path(summary_name).suffix or ".json"

    accepted_stem = Path(accepted_name).stem
    accepted_suffix = Path(accepted_name).suffix or ".csv"

    best_stem = Path(best_name).stem
    best_suffix = Path(best_name).suffix or ".json"

    for treatment in treatments:
        treatment_text = _normalize_text(treatment)
        if not treatment_text:
            continue

        tdir = rdir / "outputs" / model_id / treatment_text
        if not tdir.exists():
            continue

        found_branch_specific = False

        # -------------------------------------------------------------
        # Preferred branch-specific layout:
        #   summary__forecast_with_da.json
        #   parameters_accepted__auto_forecast_with_da.csv
        #   best__auto_forecast_with_da.json
        # -------------------------------------------------------------
        for output_type in expected_output_types:
            output_type_text = _normalize_text(output_type)
            if output_type_text not in SUPPORTED_OUTPUT_TYPES:
                continue

            summary_path = tdir / f"{summary_stem}__{output_type_text}{summary_suffix}"
            accepted_path = tdir / f"{accepted_stem}__{output_type_text}{accepted_suffix}"
            best_path = tdir / f"{best_stem}__{output_type_text}{best_suffix}"

            if summary_path.exists():
                found_branch_specific = True
                summary_obj = _load_json_if_exists(summary_path)
                _add_unique_artifact(
                    artifacts,
                    {
                        "artifact_type": "parameter_summary",
                        "model_id": model_id,
                        "treatment": treatment_text,
                        "output_type": output_type_text,
                        "series_type": output_type_text,
                        "variable": "",
                        "rel_path": _normalize_rel(summary_path, rdir),
                        "media_type": "application/json",
                        "metadata": _summary_metadata(
                            summary_obj,
                            model_id=model_id,
                            treatment=treatment_text,
                            output_type=output_type_text,
                        ),
                    },
                )

            if accepted_path.exists():
                found_branch_specific = True
                media_type = "application/json" if accepted_path.suffix.lower() == ".json" else "text/csv"
                _add_unique_artifact(
                    artifacts,
                    {
                        "artifact_type": "parameters_accepted",
                        "model_id": model_id,
                        "treatment": treatment_text,
                        "output_type": output_type_text,
                        "series_type": output_type_text,
                        "variable": "",
                        "rel_path": _normalize_rel(accepted_path, rdir),
                        "media_type": media_type,
                        "metadata": {
                            "model_id": model_id,
                            "treatment": treatment_text,
                            "output_type": output_type_text,
                            "series_type": output_type_text,
                        },
                    },
                )

            if best_path.exists():
                found_branch_specific = True
                _add_unique_artifact(
                    artifacts,
                    {
                        "artifact_type": "parameter_best",
                        "model_id": model_id,
                        "treatment": treatment_text,
                        "output_type": output_type_text,
                        "series_type": output_type_text,
                        "variable": "",
                        "rel_path": _normalize_rel(best_path, rdir),
                        "media_type": "application/json",
                        "metadata": {
                            "model_id": model_id,
                            "treatment": treatment_text,
                            "output_type": output_type_text,
                            "series_type": output_type_text,
                        },
                    },
                )

        if found_branch_specific:
            continue

        # -------------------------------------------------------------
        # Legacy layout:
        #   outputs/<model>/<treatment>/summary.json
        #   outputs/<model>/<treatment>/parameters_accepted.csv
        #   outputs/<model>/<treatment>/best.json
        # -------------------------------------------------------------
        legacy_output_type = expected_output_types[0] if expected_output_types else ""

        summary_path = tdir / summary_name
        if summary_path.exists():
            summary_obj = _load_json_if_exists(summary_path)
            _add_unique_artifact(
                artifacts,
                {
                    "artifact_type": "parameter_summary",
                    "model_id": model_id,
                    "treatment": treatment_text,
                    "output_type": legacy_output_type,
                    "series_type": legacy_output_type,
                    "variable": "",
                    "rel_path": _normalize_rel(summary_path, rdir),
                    "media_type": "application/json",
                    "metadata": _summary_metadata(
                        summary_obj,
                        model_id=model_id,
                        treatment=treatment_text,
                        output_type=legacy_output_type,
                    ),
                },
            )

        accepted_path = tdir / accepted_name
        if accepted_path.exists():
            media_type = "application/json" if accepted_path.suffix.lower() == ".json" else "text/csv"
            _add_unique_artifact(
                artifacts,
                {
                    "artifact_type": "parameters_accepted",
                    "model_id": model_id,
                    "treatment": treatment_text,
                    "output_type": legacy_output_type,
                    "series_type": legacy_output_type,
                    "variable": "",
                    "rel_path": _normalize_rel(accepted_path, rdir),
                    "media_type": media_type,
                    "metadata": {
                        "model_id": model_id,
                        "treatment": treatment_text,
                        "output_type": legacy_output_type,
                        "series_type": legacy_output_type,
                    },
                },
            )

        best_path = tdir / best_name
        if best_path.exists():
            _add_unique_artifact(
                artifacts,
                {
                    "artifact_type": "parameter_best",
                    "model_id": model_id,
                    "treatment": treatment_text,
                    "output_type": legacy_output_type,
                    "series_type": legacy_output_type,
                    "variable": "",
                    "rel_path": _normalize_rel(best_path, rdir),
                    "media_type": "application/json",
                    "metadata": {
                        "model_id": model_id,
                        "treatment": treatment_text,
                        "output_type": legacy_output_type,
                        "series_type": legacy_output_type,
                    },
                },
            )

    return artifacts


# -----------------------------------------------------------------------------
# Forecast registry payload builder
# -----------------------------------------------------------------------------
def _build_forecast_registry(
    *,
    model_id: str,
    artifacts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Build manifest.forecast_registry from discovered timeseries artifacts.
    """
    model_cfg = load_model_config(model_id)
    raw_default_publish = model_cfg.get("default_publish_variables") or []
    default_publish = {
        str(v).strip().lower()
        for v in raw_default_publish
        if str(v).strip()
    }
    publish_all = len(default_publish) == 0

    obs_map = _load_observation_map(model_id)

    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()

    for a in artifacts:
        if a.get("artifact_type") != "timeseries":
            continue

        variable = str(a.get("variable") or "").strip()
        treatment = str(a.get("treatment") or "").strip()
        output_type = str(a.get("output_type") or "").strip()
        rel_path = str(a.get("rel_path") or "").strip()
        media_type = str(a.get("media_type") or "application/json").strip()

        if not variable or not treatment or not output_type or not rel_path:
            continue

        key = (model_id, treatment, variable, output_type)
        should_publish = publish_all or (variable.lower() in default_publish)

        if not should_publish or key in seen:
            continue

        seen.add(key)

        items.append(
            {
                "model_id": model_id,
                "treatment": treatment,
                "variable": variable,
                "output_type": output_type,
                "series_type": output_type,
                "data_path": rel_path,
                "obs_path": obs_map.get(variable, ""),
                "source_ref": {
                    "rel_path": rel_path,
                    "media_type": media_type,
                    "output_type": output_type,
                    "series_type": output_type,
                },
                "is_published": 1,
            }
        )

    return items


# -----------------------------------------------------------------------------
# Public manifest builder
# -----------------------------------------------------------------------------
def build_manifest(rdir: Path) -> dict[str, Any]:
    req = load_run_request(rdir)

    run_id = str(req.get("run_id") or rdir.name)
    scheduled_task_id = req.get("scheduled_task_id")
    site_id = str(req.get("site_id") or "")
    model_id = str(req.get("model_id") or "")
    raw_task_type = str(req.get("task_type") or "")
    task_type = _normalize_task_type(raw_task_type)
    trigger_type = str(req.get("trigger_type") or "manual")

    payload = req.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    treatments = payload.get("treatments") or []
    if not isinstance(treatments, list):
        treatments = []

    expected_output_types = _resolve_expected_output_types(
        task_type=task_type,
        payload=payload,
    )

    ts_artifacts, outputs_index = _discover_timeseries_artifacts(
        rdir,
        model_id=model_id,
        treatments=treatments,
        expected_output_types=expected_output_types,
    )

    param_artifacts = _discover_parameter_artifacts(
        rdir,
        model_id=model_id,
        treatments=treatments,
        expected_output_types=expected_output_types,
    )

    artifacts = ts_artifacts + param_artifacts
    forecast_registry = _build_forecast_registry(
        model_id=model_id,
        artifacts=artifacts,
    )

    primary_output_type = expected_output_types[0] if expected_output_types else ""

    manifest = {
        "run_id": run_id,
        "scheduled_task_id": scheduled_task_id,
        "site_id": site_id,
        "model_id": model_id,
        "task_type": task_type,
        "trigger_type": trigger_type,
        "request": {
            "models": [model_id] if model_id else [],
            "treatments": treatments,
            "task": task_type,
            "payload": payload,
            "expected_output_types": expected_output_types,
            "output_type": primary_output_type,
            "series_type": primary_output_type,
            "auto_forecast_options": {
                "auto_forecast_with_da": bool(payload.get("auto_forecast_with_da", True))
                if task_type == "auto_forecast"
                else None,
                "auto_forecast_without_da": bool(
                    payload.get(
                        "auto_forecast_without_da",
                        payload.get("include_without_da", True),
                    )
                )
                if task_type == "auto_forecast"
                else None,
            },
        },
        "execution": {
            "status": "done",
        },
        "outputs": {
            "index": outputs_index,
        },
        "artifacts": artifacts,
        "forecast_registry": forecast_registry,
    }
    return manifest


def build_and_write_manifest(rdir: Path) -> dict[str, Any]:
    manifest = build_manifest(rdir)
    path = rdir / "manifest.json"
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest