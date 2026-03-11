from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from app.settings import MODELS_DIR, SITE_CONFIG_PATH


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSON config not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_site_config() -> dict[str, Any]:
    return _read_json(SITE_CONFIG_PATH)


def list_model_ids() -> list[str]:
    cfg = load_site_config()
    raw = cfg.get("models", []) or []
    out: list[str] = []
    for item in raw:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict) and item.get("id"):
            out.append(str(item["id"]))
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


def build_params_meta(model_id: str, site_id: str) -> dict[str, Any]:
    rows = load_model_parameters(model_id)
    params: list[str] = []
    param_info: dict[str, dict[str, Any]] = {}

    for row in rows:
        pid = str(row.get("id") or "").strip()
        if not pid:
            continue

        params.append(pid)
        param_info[pid] = {
            "name": str(row.get("name") or "").strip(),
            "unit": str(row.get("unit") or "").strip(),
            "default": parse_float(row.get("default")),
            "minimum": parse_float(row.get("min")),
            "maximum": parse_float(row.get("max")),
            "desc": str(row.get("desc") or "").strip(),
        }

    return {
        "site_id": site_id,
        "model_id": model_id,
        "params": params,
        "param_info": param_info,
    }


def build_site_meta() -> dict[str, Any]:
    site_cfg = load_site_config()
    site_id = str(site_cfg.get("site_id") or "").strip()

    models: list[str] = []
    model_meta: dict[str, Any] = {}
    variables_union: list[str] = []
    seen_vars: set[str] = set()

    for model_id in list_model_ids():
        models.append(model_id)

        model_cfg = load_model_config(model_id)
        vars_cfg = load_model_variables(model_id)

        enabled_tasks: list[str] = []
        tasks_block = model_cfg.get("tasks", {}) or {}
        if isinstance(tasks_block, dict):
            for task_name, task_cfg in tasks_block.items():
                if isinstance(task_cfg, dict) and task_cfg.get("enabled", False):
                    enabled_tasks.append(str(task_name))

        var_names: list[str] = []
        for item in vars_cfg:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            var_names.append(name)
            if name not in seen_vars:
                seen_vars.add(name)
                variables_union.append(name)

        model_meta[model_id] = {
            "name": str(model_cfg.get("name") or model_id),
            "description": str(model_cfg.get("description") or ""),
            "tasks": enabled_tasks,
            "variables": var_names,
            "treatments": site_cfg.get("treatments", []) or [],
            "default_publish_variables": model_cfg.get("default_publish_variables", []) or [],
        }

    return {
        "site_id": site_id,
        "site_name": str(site_cfg.get("site_name") or site_cfg.get("name") or site_id),
        "description": str(site_cfg.get("description") or ""),
        "models": models,
        "model_meta": model_meta,
        "variables": variables_union,
        "treatments": site_cfg.get("treatments", []) or [],
        "default_model": site_cfg.get("default_model") or (models[0] if models else ""),
    }


def resolve_task_command(
    *,
    model_id: str,
    task_type: str,
    run_dir: str,
) -> list[str]:
    model_cfg = load_model_config(model_id)
    tasks_block = model_cfg.get("tasks", {}) or {}
    task_cfg = tasks_block.get(task_type)

    if not isinstance(task_cfg, dict):
        raise ValueError(f"Task '{task_type}' not found for model '{model_id}'")
    if not task_cfg.get("enabled", False):
        raise ValueError(f"Task '{task_type}' is disabled for model '{model_id}'")

    cmd = task_cfg.get("command", [])
    if not isinstance(cmd, list) or not cmd:
        raise ValueError(f"Task '{task_type}' command is invalid for model '{model_id}'")

    rendered: list[str] = []
    for token in cmd:
        s = str(token)
        s = s.replace("{run_dir}", run_dir)
        s = s.replace("{model_id}", model_id)
        s = s.replace("{task_type}", task_type)
        rendered.append(s)

    return rendered


def load_run_request(rdir: Path) -> dict[str, Any]:
    path = rdir / "request.json"
    if not path.exists():
        raise FileNotFoundError(f"request.json not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))