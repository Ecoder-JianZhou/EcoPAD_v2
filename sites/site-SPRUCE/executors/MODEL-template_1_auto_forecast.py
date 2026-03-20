#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def _find_request_path(arg: str | None) -> Path:
    if arg is None:
        cwd = Path.cwd()
        for name in ("request.json", "run_request.json"):
            p = cwd / name
            if p.exists():
                return p
        return cwd / "request.json"

    p = Path(arg)
    if p.is_dir():
        for name in ("request.json", "run_request.json"):
            rp = p / name
            if rp.exists():
                return rp
        return p / "request.json"

    return p


def _resolve_run_dir(request_path: Path) -> Path:
    return request_path.parent


def _pick_first(d: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _candidate_model_dirs(model_id: str) -> list[Path]:
    return [
        Path("/app/config/models") / model_id,
        Path("/app/config") / "models" / model_id,
        Path("/app/config") / model_id,
    ]


def _find_model_dir(model_id: str) -> Path:
    for p in _candidate_model_dirs(model_id):
        if p.exists() and p.is_dir():
            return p
    raise FileNotFoundError(
        f"Model config dir not found for model_id='{model_id}'. "
        f"Tried: {[str(p) for p in _candidate_model_dirs(model_id)]}"
    )


def _load_variables(model_dir: Path) -> list[dict[str, Any]]:
    path = model_dir / "variables.json"
    if not path.exists():
        return []

    data = _read_json(path)
    items = data.get("variables", [])
    out: list[dict[str, Any]] = []
    for item in items:
        vid = item.get("id") or item.get("name") or item.get("label")
        if not vid:
            continue
        out.append(
            {
                "id": str(vid),
                "label": str(item.get("label") or item.get("full_name") or vid),
                "unit": str(item.get("unit") or ""),
            }
        )
    return out


def _load_parameters(model_dir: Path) -> list[dict[str, Any]]:
    path = model_dir / "parameters.csv"
    if not path.exists():
        raise FileNotFoundError(f"Parameter file not found: {path}")

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            pid = raw.get("id") or raw.get("name")
            label = raw.get("name") or raw.get("full_name") or pid
            if not pid:
                continue
            rows.append(
                {
                    "id": str(pid),
                    "name": str(label or pid),
                    "unit": str(raw.get("unit") or ""),
                    "default": _to_float(raw.get("default"), 0.0),
                    "min": _to_float(raw.get("min"), _to_float(raw.get("default"), 0.0)),
                    "max": _to_float(raw.get("max"), _to_float(raw.get("default"), 0.0)),
                    "desc": str(raw.get("desc") or raw.get("description") or ""),
                }
            )
    return rows


def _date_series(n: int = 60) -> list[str]:
    start = datetime(2026, 1, 1)
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n)]


def _default_base_value(variable: str) -> float:
    table = {
        "GPP": 8.0,
        "ER": 6.5,
        "NEE": -1.2,
        "NPP": 3.8,
        "LAI": 2.4,
    }
    return table.get(variable, 5.0)


def _task_offset(task_type: str) -> float:
    return {
        "simulate": 0.0,
        "forecast": 0.8,
        "auto_forecast": 1.2,
        "mcmc": -0.3,
    }.get(task_type, 0.5)


def _treatment_offset(treatment: str) -> float:
    return (sum(ord(c) for c in treatment) % 9) * 0.12


def _parameter_effect(best_params: dict[str, float]) -> float:
    vcmax = best_params.get("Vcmax", best_params.get("vcmax", 50.0))
    rl0 = best_params.get("Rl0", best_params.get("rl0", 0.015))
    q10 = best_params.get("Q10", best_params.get("q10", 2.0))
    return 0.01 * (vcmax - 50.0) - 5.0 * (rl0 - 0.015) + 0.2 * (q10 - 2.0)


def _make_series(
    *,
    variable: str,
    unit: str,
    model_id: str,
    treatment: str,
    task_type: str,
    best_params: dict[str, float],
    n: int = 60,
) -> dict[str, Any]:
    dates = _date_series(n)

    base = _default_base_value(variable)
    off_task = _task_offset(task_type)
    off_treatment = _treatment_offset(treatment)
    off_param = _parameter_effect(best_params)

    mean: list[float] = []
    lo: list[float] = []
    hi: list[float] = []

    for i in range(n):
        seasonal = 0.35 * math.sin(i / 8.0)
        trend = i * 0.03
        v = round(base + off_task + off_treatment + off_param + seasonal + trend, 4)
        mean.append(v)
        lo.append(round(v - 0.4, 4))
        hi.append(round(v + 0.4, 4))

    return {
        "variable": variable,
        "units": unit,
        "series": [
            {
                "model": model_id,
                "treatment": treatment,
                "time": dates,
                "mean": mean,
                "lo": lo,
                "hi": hi,
            }
        ],
    }


def _normalize_override_parameters(payload: dict[str, Any]) -> dict[str, float]:
    raw = payload.get("parameters", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in raw.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            continue
    return out


def _generate_accepted_samples(
    param_defs: list[dict[str, Any]],
    overrides: dict[str, float],
    n: int = 200,
) -> tuple[list[str], list[list[Any]], list[dict[str, float]], dict[str, float]]:
    header = ["sample_id"] + [p["id"] for p in param_defs]
    rows: list[list[Any]] = []
    samples: list[dict[str, float]] = []

    for i in range(n):
        frac = i / max(n - 1, 1)
        row = [i + 1]
        item: dict[str, float] = {}

        for p in param_defs:
            pid = p["id"]
            pmin = float(p["min"])
            pmax = float(p["max"])
            pdef = float(p["default"])

            if pid in overrides:
                center = overrides[pid]
            elif p["name"] in overrides:
                center = overrides[p["name"]]
            else:
                center = pdef

            if pmax <= pmin:
                value = pdef
            else:
                width = (pmax - pmin) * 0.15
                low = max(pmin, center - width)
                high = min(pmax, center + width)

                if high <= low:
                    value = max(pmin, min(pmax, center))
                else:
                    wave = (math.sin(i * 0.37 + len(pid)) + 1.0) / 2.0
                    blend = 0.65 * frac + 0.35 * wave
                    value = low + (high - low) * blend

            value = round(value, 6)
            row.append(value)
            item[pid] = value

        rows.append(row)
        samples.append(item)

    best = samples[-1] if samples else {}
    return header, rows, samples, best


def _build_summary(
    param_defs: list[dict[str, Any]],
    samples: list[dict[str, float]],
    task_type: str,
    model_id: str,
) -> dict[str, Any]:
    params_out: list[dict[str, Any]] = []

    for p in param_defs:
        pid = p["id"]
        vals = [s[pid] for s in samples if pid in s]
        if vals:
            mean_v = sum(vals) / len(vals)
            min_v = min(vals)
            max_v = max(vals)
            last_v = vals[-1]
        else:
            mean_v = p["default"]
            min_v = p["default"]
            max_v = p["default"]
            last_v = p["default"]

        params_out.append(
            {
                "id": pid,
                "name": p["name"],
                "label": p["name"],
                "unit": p["unit"],
                "default": p["default"],
                "min": p["min"],
                "max": p["max"],
                "desc": p["desc"],
                "value": round(last_v, 6),
                "mean": round(mean_v, 6),
                "accepted_min": round(min_v, 6),
                "accepted_max": round(max_v, 6),
            }
        )

    return {
        "model_id": model_id,
        "task_type": task_type,
        "n_accepted": len(samples),
        "parameters": params_out,
    }


def main() -> int:
    request_path = _find_request_path(sys.argv[1] if len(sys.argv) > 1 else None)
    run_dir = _resolve_run_dir(request_path)
    req = _read_json(request_path)

    payload = req.get("payload") if isinstance(req.get("payload"), dict) else req
    if not isinstance(payload, dict):
        payload = {}

    model_id = str(
        _pick_first(
            req,
            ["model_id", "model"],
            _pick_first(payload, ["model_id", "model"], "MODEL-template_1"),
        )
    )
    task_type = str(
        _pick_first(
            req,
            ["task_type", "task"],
            _pick_first(payload, ["task_type", "task"], "auto_forecast"),
        )
    )

    treatments = payload.get("treatments") or req.get("treatments") or []
    if not treatments:
        raise ValueError("No treatments provided in request payload.")

    model_dir = _find_model_dir(model_id)
    variable_defs = _load_variables(model_dir)
    param_defs = _load_parameters(model_dir)

    requested_variables = payload.get("variables") or req.get("variables") or []
    if requested_variables:
        variables = [str(v) for v in requested_variables]
    elif variable_defs:
        variables = [v["id"] for v in variable_defs]
    else:
        variables = ["GPP", "ER", "NEE"]

    unit_map = {v["id"]: v.get("unit", "") for v in variable_defs}

    overrides = _normalize_override_parameters(payload)
    _, accepted_rows, accepted_samples, best_params = _generate_accepted_samples(
        param_defs=param_defs,
        overrides=overrides,
        n=200,
    )

    summary = _build_summary(
        param_defs=param_defs,
        samples=accepted_samples,
        task_type=task_type,
        model_id=model_id,
    )

    best_json = {
        "model_id": model_id,
        "task_type": task_type,
        "parameter_count": len(best_params),
        "parameters": [
            {
                "id": p["id"],
                "name": p["name"],
                "label": p["name"],
                "unit": p["unit"],
                "value": best_params.get(p["id"], p["default"]),
            }
            for p in param_defs
        ],
    }

    for treatment in treatments:
        tdir = run_dir / "outputs" / model_id / str(treatment)
        tdir.mkdir(parents=True, exist_ok=True)

        for variable in variables:
            obj = _make_series(
                variable=variable,
                unit=unit_map.get(variable, ""),
                model_id=model_id,
                treatment=str(treatment),
                task_type=task_type,
                best_params=best_params,
                n=60,
            )
            _write_json(tdir / f"{variable}.json", obj)

        accepted_header = ["sample_id"] + [p["id"] for p in param_defs]
        _write_csv(tdir / "Parameters_accepted.csv", accepted_header, accepted_rows)
        _write_json(tdir / "summary.json", summary)
        _write_json(tdir / "best.json", best_json)

    print(f"[ok] auto_forecast finished: run_dir={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())