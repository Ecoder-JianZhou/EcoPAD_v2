from __future__ import annotations

import datetime
import json
import math
import random
import sys
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------
# Canonical output branches actually written by this executor
# ---------------------------------------------------------------------
STANDARD_OUTPUT_TYPES = (
    "simulation_without_da",
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
)

AUTO_FORECAST_OUTPUT_TYPES = (
    "auto_forecast_with_da",
    "auto_forecast_without_da",
)

ALL_OUTPUT_TYPES = STANDARD_OUTPUT_TYPES + AUTO_FORECAST_OUTPUT_TYPES


# ---------------------------------------------------------------------
# Basic helpers
# ---------------------------------------------------------------------
def load_request(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "request.json"
    if not path.exists():
        raise FileNotFoundError(f"request.json not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_bool(value: Any, default: bool = False) -> bool:
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


def normalize_task_type(value: Any) -> str:
    """
    Normalize incoming task_type to canonical internal names.
    """
    text = normalize_text(value).lower()

    mapping = {
        # old
        "simulate": "simulation_without_da",
        "simulation_with_da": "simulation_with_da",
        "forecast_with_da": "forecast_with_da",
        "forecast_without_da": "forecast_without_da",
        "auto_forecast": "auto_forecast",

        # new / display-like
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


def normalize_requested_mode(value: Any) -> str:
    """
    Compatibility helper for older payload.series_type style.
    """
    text = normalize_text(value).lower()
    mapping = {
        "forecast_with_da": "forecast_with_da",
        "forecast_without_da": "forecast_without_da",
        "with_da": "forecast_with_da",
        "without_da": "forecast_without_da",
    }
    return mapping.get(text, "")


# ---------------------------------------------------------------------
# Output-type resolution
# ---------------------------------------------------------------------
def resolve_output_types(task_type: str, payload: dict[str, Any]) -> list[str]:
    """
    Resolve which output branch(es) this run should write.

    Rules:
    - simulation_without_da -> ["simulation_without_da"]
    - simulation_with_da    -> ["simulation_with_da"]
    - forecast_with_da      -> ["forecast_with_da"]
    - forecast_without_da   -> ["forecast_without_da"]
    - auto_forecast         -> one or both of:
        ["auto_forecast_with_da", "auto_forecast_without_da"]

    Current preferred control for auto_forecast:
    - payload.auto_forecast_with_da      default True
    - payload.auto_forecast_without_da   default True

    Compatibility:
    - older payload.series_type may request only one forecast mode
    """
    task = normalize_task_type(task_type)
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
        requested_mode = normalize_requested_mode(payload.get("series_type"))
        if requested_mode == "forecast_with_da":
            return ["auto_forecast_with_da"]
        if requested_mode == "forecast_without_da":
            return ["auto_forecast_without_da"]

        enable_with_da = normalize_bool(payload.get("auto_forecast_with_da"), True)
        enable_without_da = normalize_bool(payload.get("auto_forecast_without_da"), True)

        out: list[str] = []
        if enable_with_da:
            out.append("auto_forecast_with_da")
        if enable_without_da:
            out.append("auto_forecast_without_da")

        if not out:
            raise ValueError(
                "auto_forecast requested, but both auto_forecast_with_da and "
                "auto_forecast_without_da are disabled in payload"
            )

        return out

    raise ValueError(f"Unsupported task_type: {task or '<empty>'}")


# ---------------------------------------------------------------------
# Mock data builders
# ---------------------------------------------------------------------
def write_timeseries_json(path: Path, rows: list[dict[str, Any]]) -> None:
    """
    Write raw mock rows.

    Note:
    current site reader may later transform these into plotting format.
    """
    path.write_text(
        json.dumps(rows, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def build_mock_series(
    *,
    start: datetime.date,
    n_days: int = 365,
    output_type: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Build slightly different mock series by output_type,
    so plots can visually distinguish branches.
    """
    gpp_data: list[dict[str, Any]] = []
    er_data: list[dict[str, Any]] = []
    nee_data: list[dict[str, Any]] = []

    if output_type in {"simulation_with_da", "forecast_with_da", "auto_forecast_with_da"}:
        gpp_shift = 1.2
        er_shift = 0.4
    elif output_type in {"forecast_without_da", "auto_forecast_without_da"}:
        gpp_shift = -0.4
        er_shift = 0.2
    else:
        gpp_shift = 0.0
        er_shift = 0.0

    for i in range(n_days):
        d = start + datetime.timedelta(days=i)

        seasonal = 2.0 + 3.0 * (1 + math.sin(2 * math.pi * i / 365.0)) / 2
        gpp = random.uniform(0, 5) + seasonal + gpp_shift
        er = random.uniform(0, 4) + seasonal * 0.6 + er_shift
        nee = gpp - er

        gpp_data.append({"date": str(d), "value": round(gpp, 4)})
        er_data.append({"date": str(d), "value": round(er, 4)})
        nee_data.append({"date": str(d), "value": round(nee, 4)})

    return gpp_data, er_data, nee_data


def build_mock_parameter_summary(
    *,
    req: dict[str, Any],
    model_id: str,
    task_type: str,
    output_type: str,
    treatment: str,
) -> dict[str, Any]:
    if output_type in {"simulation_with_da", "forecast_with_da", "auto_forecast_with_da"}:
        vcmax_opt = 42.1
        rl0_opt = 0.92
    elif output_type in {"forecast_without_da", "auto_forecast_without_da"}:
        vcmax_opt = 39.4
        rl0_opt = 0.97
    else:
        vcmax_opt = 45.0
        rl0_opt = 0.80

    return {
        "run_id": req.get("run_id"),
        "site_id": req.get("site_id"),
        "model_id": model_id,
        "task_type": task_type,
        "output_type": output_type,
        "treatment": treatment,
        "parameter_estimate_method": "mock_default",
        "parameters": [
            {
                "id": "vcmax",
                "name": "Maximum carboxylation rate",
                "unit": "umol m-2 s-1",
                "default": 45.0,
                "optimized": vcmax_opt,
                "map": round(vcmax_opt - 0.3, 4),
                "mean": vcmax_opt,
                "median": round(vcmax_opt - 0.1, 4),
                "sd": 3.5,
                "p05": round(vcmax_opt - 5.9, 4),
                "p25": round(vcmax_opt - 2.3, 4),
                "p75": round(vcmax_opt + 2.2, 4),
                "p95": round(vcmax_opt + 6.5, 4),
                "minimum": 10.0,
                "maximum": 120.0,
            },
            {
                "id": "rl0",
                "name": "Base leaf respiration rate",
                "unit": "umol m-2 s-1",
                "default": 0.8,
                "optimized": rl0_opt,
                "map": round(rl0_opt - 0.02, 4),
                "mean": rl0_opt,
                "median": round(rl0_opt - 0.01, 4),
                "sd": 0.08,
                "p05": round(rl0_opt - 0.13, 4),
                "p25": round(rl0_opt - 0.06, 4),
                "p75": round(rl0_opt + 0.06, 4),
                "p95": round(rl0_opt + 0.13, 4),
                "minimum": 0.1,
                "maximum": 3.0,
            },
        ],
    }


def build_mock_best(
    *,
    req: dict[str, Any],
    model_id: str,
    task_type: str,
    output_type: str,
    treatment: str,
) -> dict[str, Any]:
    if output_type in {"simulation_with_da", "forecast_with_da", "auto_forecast_with_da"}:
        vcmax_value = 42.1
        rl0_value = 0.92
    elif output_type in {"forecast_without_da", "auto_forecast_without_da"}:
        vcmax_value = 39.4
        rl0_value = 0.97
    else:
        vcmax_value = 45.0
        rl0_value = 0.80

    return {
        "run_id": req.get("run_id"),
        "site_id": req.get("site_id"),
        "model_id": model_id,
        "task_type": task_type,
        "output_type": output_type,
        "treatment": treatment,
        "parameter_count": 2,
        "parameters": [
            {
                "id": "vcmax",
                "name": "Maximum carboxylation rate",
                "label": "Maximum carboxylation rate",
                "unit": "umol m-2 s-1",
                "value": vcmax_value,
            },
            {
                "id": "rl0",
                "name": "Base leaf respiration rate",
                "label": "Base leaf respiration rate",
                "unit": "umol m-2 s-1",
                "value": rl0_value,
            },
        ],
    }


def write_mock_parameters_accepted(
    path: Path,
    *,
    output_type: str,
) -> None:
    rows = [
        "vcmax,rl0",
        "41.5,0.88",
        "42.0,0.90",
        "42.3,0.93",
        "41.9,0.91",
        "42.6,0.95",
    ]

    if output_type in {"forecast_without_da", "auto_forecast_without_da"}:
        rows = [
            "vcmax,rl0",
            "38.8,0.93",
            "39.2,0.95",
            "39.5,0.97",
            "39.7,0.99",
            "39.1,0.96",
        ]

    if output_type == "simulation_without_da":
        rows = [
            "vcmax,rl0",
            "44.5,0.79",
            "45.1,0.81",
            "44.8,0.80",
            "45.3,0.82",
            "44.9,0.80",
        ]

    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------
def write_one_output_branch(
    *,
    run_dir: Path,
    req: dict[str, Any],
    model_id: str,
    task_type: str,
    treatment: str,
    output_type: str,
    start: datetime.date,
) -> None:
    """
    Write one concrete output branch.

    Directory rules:
    - normal tasks:
        outputs/<model>/<treatment>/<output_type>/*.json
    - auto_forecast branches:
        outputs/<model>/<treatment>/auto_forecast_with_da/*.json
        outputs/<model>/<treatment>/auto_forecast_without_da/*.json

    Parameter files:
    - normal tasks use legacy filenames for compatibility
    - auto_forecast uses branch-specific filenames so both branches coexist
    """
    if output_type not in ALL_OUTPUT_TYPES:
        raise ValueError(f"Unsupported output_type: {output_type}")

    series_out_dir = run_dir / "outputs" / model_id / treatment / output_type
    series_out_dir.mkdir(parents=True, exist_ok=True)

    treatment_root_dir = run_dir / "outputs" / model_id / treatment
    treatment_root_dir.mkdir(parents=True, exist_ok=True)

    gpp_data, er_data, nee_data = build_mock_series(
        start=start,
        n_days=365,
        output_type=output_type,
    )

    write_timeseries_json(series_out_dir / "GPP.json", gpp_data)
    write_timeseries_json(series_out_dir / "ER.json", er_data)
    write_timeseries_json(series_out_dir / "NEE.json", nee_data)

    summary = build_mock_parameter_summary(
        req=req,
        model_id=model_id,
        task_type=task_type,
        output_type=output_type,
        treatment=treatment,
    )
    best = build_mock_best(
        req=req,
        model_id=model_id,
        task_type=task_type,
        output_type=output_type,
        treatment=treatment,
    )

    if task_type == "auto_forecast":
        summary_name = f"summary__{output_type}.json"
        best_name = f"best__{output_type}.json"
        accepted_name = f"parameters_accepted__{output_type}.csv"
    else:
        summary_name = "summary.json"
        best_name = "best.json"
        accepted_name = "parameters_accepted.csv"

    (treatment_root_dir / summary_name).write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (treatment_root_dir / best_name).write_text(
        json.dumps(best, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_mock_parameters_accepted(
        treatment_root_dir / accepted_name,
        output_type=output_type,
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python /app/executors/MODEL-template_1_simulate.py <run_dir>")

    run_dir = Path(sys.argv[1])
    req = load_request(run_dir)

    model_id = normalize_text(req.get("model_id"))
    raw_task_type = normalize_text(req.get("task_type"))
    task_type = normalize_task_type(raw_task_type)
    payload = req.get("payload") or {}
    treatments = payload.get("treatments") or []

    if not model_id:
        raise ValueError("model_id missing in request.json")

    expected_model_id = "TECO-SPRUCE_v2"
    if model_id != expected_model_id:
        raise ValueError(
            f"executor MODEL-template_1_simulate.py received mismatched model_id={model_id!r}"
        )

    if not isinstance(treatments, list) or not treatments:
        raise ValueError("payload.treatments missing in request.json")

    output_types = resolve_output_types(task_type, payload)
    start = datetime.date(2020, 1, 1)

    for treatment in treatments:
        treatment_text = normalize_text(treatment)
        if not treatment_text:
            continue

        for output_type in output_types:
            write_one_output_branch(
                run_dir=run_dir,
                req=req,
                model_id=model_id,
                task_type=task_type,
                treatment=treatment_text,
                output_type=output_type,
                start=start,
            )

    summary_obj = {
        "run_id": req.get("run_id"),
        "site_id": req.get("site_id"),
        "model_id": model_id,
        "task_type": task_type,
        "treatments": [normalize_text(x) for x in treatments if normalize_text(x)],
        "output_types": output_types,
        "status": "done",
    }
    (run_dir / "executor_summary.json").write_text(
        json.dumps(summary_obj, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()