from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from app.settings import MODELS_DIR, SITE_CONFIG_PATH


SUPPORTED_OUTPUT_TYPES = (
    "simulation_without_da",
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
    "auto_forecast_with_da",
    "auto_forecast_without_da",
)

LEGACY_TASK_ALIASES = {
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

LEGACY_OUTPUT_ALIASES = {
    "simulate": "simulation_without_da",
    "simulation_without_da": "simulation_without_da",
    "simulation_with_da": "simulation_with_da",
    "forecast_with_da": "forecast_with_da",
    "forecast_without_da": "forecast_without_da",
    "auto_forecast_with_da": "auto_forecast_with_da",
    "auto_forecast_without_da": "auto_forecast_without_da",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSON config not found: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"JSON config must be an object: {path}")
    return obj


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_task_type(value: Any) -> str:
    text = _normalize_text(value).lower()
    return LEGACY_TASK_ALIASES.get(text, text)


def _normalize_output_type(value: Any, default: str = "") -> str:
    text = _normalize_text(value).lower()
    if not text:
        return default
    out = LEGACY_OUTPUT_ALIASES.get(text, text)
    if out not in SUPPORTED_OUTPUT_TYPES:
        return default
    return out


def load_site_config() -> dict[str, Any]:
    return _read_json(SITE_CONFIG_PATH)


def list_model_ids() -> list[str]:
    cfg = load_site_config()
    raw = cfg.get("models", []) or []
    out: list[str] = []
    for item in raw:
        if isinstance(item, str):
            text = item.strip()
            if text:
                out.append(text)
        elif isinstance(item, dict) and item.get("id"):
            out.append(str(item["id"]).strip())
    return out


def model_dir(model_id: str) -> Path:
    return MODELS_DIR / model_id


def load_model_config(model_id: str) -> dict[str, Any]:
    return _read_json(model_dir(model_id) / "model.json")


def load_model_variables(model_id: str) -> list[dict[str, Any]]:
    obj = _read_json(model_dir(model_id) / "variables.json")
    raw = obj.get("variables", []) or []
    return [x for x in raw if isinstance(x, dict)]


def load_model_parameters(model_id: str) -> list[dict[str, Any]]:
    path = model_dir(model_id) / "parameters.csv"
    if not path.exists():
        raise FileNotFoundError(f"Parameter CSV not found: {path}")

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _output_types_from_enabled_tasks(enabled_tasks: list[str]) -> list[str]:
    """
    Infer supported output types from enabled task names.

    Mapping:
    - simulation_without_da -> simulation_without_da
    - simulation_with_da -> simulation_with_da
    - forecast_with_da -> forecast_with_da
    - forecast_without_da -> forecast_without_da
    - auto_forecast -> auto_forecast_with_da + auto_forecast_without_da
    """
    out: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        if value in SUPPORTED_OUTPUT_TYPES and value not in seen:
            seen.add(value)
            out.append(value)

    for task_name in enabled_tasks:
        task = _normalize_task_type(task_name)

        if task == "simulation_without_da":
            add("simulation_without_da")
        elif task == "simulation_with_da":
            add("simulation_with_da")
        elif task == "forecast_with_da":
            add("forecast_with_da")
        elif task == "forecast_without_da":
            add("forecast_without_da")
        elif task == "auto_forecast":
            add("auto_forecast_with_da")
            add("auto_forecast_without_da")

    return out


def _load_observations_meta(model_cfg: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Build observation metadata map keyed by variable.
    """
    out: dict[str, dict[str, Any]] = {}
    raw = model_cfg.get("observations") or []

    if not isinstance(raw, list):
        return out

    for item in raw:
        if not isinstance(item, dict):
            continue

        variable = _normalize_text(item.get("variable") or item.get("id"))
        if not variable:
            continue

        out[variable] = {
            "variable": variable,
            "path": _normalize_text(item.get("path")),
            "time_resolution": _normalize_text(item.get("time_resolution")),
            "unit": _normalize_text(item.get("unit")),
        }

    return out


def build_params_meta(model_id: str, site_id: str) -> dict[str, Any]:
    rows = load_model_parameters(model_id)
    params: list[str] = []
    param_info: dict[str, dict[str, Any]] = {}

    for row in rows:
        pid = _normalize_text(row.get("id"))
        if not pid:
            continue

        params.append(pid)
        param_info[pid] = {
            "id": pid,
            "name": _normalize_text(row.get("name")),
            "full": _normalize_text(row.get("name")),
            "symbol": _normalize_text(row.get("symbol") or pid),
            "unit": _normalize_text(row.get("unit")),
            "default": parse_float(row.get("default")),
            "minimum": parse_float(row.get("min")),
            "maximum": parse_float(row.get("max")),
            "min": parse_float(row.get("min")),
            "max": parse_float(row.get("max")),
            "desc": _normalize_text(row.get("desc")),
            "description": _normalize_text(row.get("desc")),
            "category": _normalize_text(row.get("category")),
            "group": _normalize_text(row.get("group")),
            "module": _normalize_text(row.get("module")),
            "dimension": _normalize_text(row.get("dimension")),
            "source": _normalize_text(row.get("source")),
        }

    return {
        "site_id": site_id,
        "model_id": model_id,
        "params": params,
        "param_info": param_info,
    }


def build_site_meta() -> dict[str, Any]:
    site_cfg = load_site_config()
    site_id = _normalize_text(site_cfg.get("site_id"))

    models: list[str] = []
    model_meta: dict[str, Any] = {}
    variables_union: list[str] = []
    seen_vars: set[str] = set()
    output_types_union: list[str] = []
    seen_output_types: set[str] = set()
    observations_union: dict[str, dict[str, Any]] = {}

    for model_id in list_model_ids():
        models.append(model_id)

        model_cfg = load_model_config(model_id)
        vars_cfg = load_model_variables(model_id)
        obs_meta = _load_observations_meta(model_cfg)

        enabled_tasks: list[str] = []
        tasks_block = model_cfg.get("tasks", {}) or {}
        if isinstance(tasks_block, dict):
            for task_name, task_cfg in tasks_block.items():
                if isinstance(task_cfg, dict) and task_cfg.get("enabled", False):
                    enabled_tasks.append(_normalize_task_type(task_name))

        var_names: list[str] = []
        variable_info: dict[str, dict[str, Any]] = {}

        for item in vars_cfg:
            if not isinstance(item, dict):
                continue

            vid = _normalize_text(item.get("id"))
            name = _normalize_text(item.get("name") or vid)
            if not name:
                continue

            var_names.append(name)
            if name not in seen_vars:
                seen_vars.add(name)
                variables_union.append(name)

            variable_info[name] = {
                "id": vid or name,
                "name": name,
                "unit": _normalize_text(item.get("unit")),
                "desc": _normalize_text(item.get("desc") or item.get("description")),
                "description": _normalize_text(item.get("desc") or item.get("description")),
                "filename": _normalize_text(item.get("filename") or item.get("output_file")),
            }

        supported_output_types = _output_types_from_enabled_tasks(enabled_tasks)
        for ot in supported_output_types:
            if ot not in seen_output_types:
                seen_output_types.add(ot)
                output_types_union.append(ot)

        for vname, meta in obs_meta.items():
            if vname not in observations_union:
                observations_union[vname] = meta

        model_meta[model_id] = {
            "name": _normalize_text(model_cfg.get("name") or model_id),
            "description": _normalize_text(model_cfg.get("description")),
            "tasks": enabled_tasks,
            "output_types": supported_output_types,
            "series_types": supported_output_types,  # backward compatibility
            "variables": var_names,
            "variable_info": variable_info,
            "treatments": site_cfg.get("treatments", []) or [],
            "default_publish_variables": model_cfg.get("default_publish_variables", []) or [],
            "observations": list(obs_meta.values()),
            "observation_map": obs_meta,
        }

    return {
        "site_id": site_id,
        "site_name": _normalize_text(site_cfg.get("site_name") or site_cfg.get("name") or site_id),
        "description": _normalize_text(site_cfg.get("description")),
        "models": models,
        "model_meta": model_meta,
        "variables": variables_union,
        "treatments": site_cfg.get("treatments", []) or [],
        "output_types": output_types_union,
        "series_types": output_types_union,  # backward compatibility
        "observations": list(observations_union.values()),
        "observation_map": observations_union,
        "default_model": site_cfg.get("default_model") or (models[0] if models else ""),
    }


def resolve_task_command(
    *,
    model_id: str,
    task_type: str,
    run_dir: str,
) -> list[str]:
    """
    Resolve executable command for one task.

    Compatibility:
    - Runner may pass canonical task names
    - model.json may still use old task key names
    """
    model_cfg = load_model_config(model_id)
    tasks_block = model_cfg.get("tasks", {}) or {}
    if not isinstance(tasks_block, dict):
        raise ValueError(f"Invalid tasks block for model '{model_id}'")

    canonical_task = _normalize_task_type(task_type)

    # primary lookup: exact canonical key
    task_cfg = tasks_block.get(canonical_task)

    # compatibility lookup
    if not isinstance(task_cfg, dict):
        compat_keys = []
        if canonical_task == "simulation_without_da":
            compat_keys = ["simulate"]
        elif canonical_task == "simulation_with_da":
            compat_keys = ["simulation_with_da"]
        elif canonical_task == "forecast_with_da":
            compat_keys = ["forecast_with_da"]
        elif canonical_task == "forecast_without_da":
            compat_keys = ["forecast_without_da"]
        elif canonical_task == "auto_forecast":
            compat_keys = ["auto_forecast"]

        for key in compat_keys:
            cfg = tasks_block.get(key)
            if isinstance(cfg, dict):
                task_cfg = cfg
                break

    if not isinstance(task_cfg, dict):
        raise ValueError(f"Task '{canonical_task}' not found for model '{model_id}'")

    if not task_cfg.get("enabled", False):
        raise ValueError(f"Task '{canonical_task}' is disabled for model '{model_id}'")

    cmd = task_cfg.get("command", [])
    if not isinstance(cmd, list) or not cmd:
        raise ValueError(f"Task '{canonical_task}' command is invalid for model '{model_id}'")

    rendered: list[str] = []
    for token in cmd:
        s = str(token)
        s = s.replace("{run_dir}", run_dir)
        s = s.replace("{model_id}", model_id)
        s = s.replace("{task_type}", canonical_task)
        rendered.append(s)

    return rendered


def load_run_request(rdir: Path) -> dict[str, Any]:
    path = rdir / "request.json"
    if not path.exists():
        raise FileNotFoundError(f"request.json not found: {path}")

    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"Invalid request.json format: {path}")
    return obj