from __future__ import annotations

import json
import random
import sys
import datetime
from pathlib import Path
from typing import Any


def load_request(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "request.json"
    if not path.exists():
        raise FileNotFoundError(f"request.json not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python /app/executors/MODEL-template_1_simulate.py <run_dir>")

    run_dir = Path(sys.argv[1])
    req = load_request(run_dir)

    model_id = str(req.get("model_id") or "").strip()
    task_type = str(req.get("task_type") or "").strip()
    payload = req.get("payload") or {}
    treatments = payload.get("treatments") or []

    if not model_id:
        raise ValueError("model_id missing in request.json")

    expected_model_id = "MODEL-template_1"
    if model_id != expected_model_id:
        raise ValueError(
            f"executor MODEL-template_1_simulate.py received mismatched model_id={model_id!r}"
        )

    if not isinstance(treatments, list) or not treatments:
        raise ValueError("payload.treatments missing in request.json")

    start = datetime.date(2020, 1, 1)

    for treatment in treatments:
        out_dir = run_dir / "outputs" / model_id / treatment
        out_dir.mkdir(parents=True, exist_ok=True)

        gpp_data = []
        er_data = []
        nee_data = []

        for i in range(365):
            d = start + datetime.timedelta(days=i)
            gpp = random.uniform(0, 10)
            er = random.uniform(0, 8)
            nee = gpp - er

            gpp_data.append({"date": str(d), "value": round(gpp, 4)})
            er_data.append({"date": str(d), "value": round(er, 4)})
            nee_data.append({"date": str(d), "value": round(nee, 4)})

        (out_dir / "GPP.json").write_text(
            json.dumps(gpp_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        (out_dir / "ER.json").write_text(
            json.dumps(er_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        (out_dir / "NEE.json").write_text(
            json.dumps(nee_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        summary = {
            "run_id": req.get("run_id"),
            "site_id": req.get("site_id"),
            "model_id": model_id,
            "task_type": task_type,
            "treatment": treatment,
            "parameter_estimate_method": "mock_default",
            "parameters": [
                {
                    "id": "vcmax",
                    "name": "Maximum carboxylation rate",
                    "unit": "umol m-2 s-1",
                    "default": 45.0,
                    "optimized": 42.1,
                    "map": 41.8,
                    "mean": 42.1,
                    "median": 42.0,
                    "sd": 3.5,
                    "p05": 36.2,
                    "p25": 39.8,
                    "p75": 44.3,
                    "p95": 48.6,
                    "minimum": 10.0,
                    "maximum": 120.0
                },
                {
                    "id": "rl0",
                    "name": "Base leaf respiration rate",
                    "unit": "umol m-2 s-1",
                    "default": 0.8,
                    "optimized": 0.92,
                    "map": 0.90,
                    "mean": 0.92,
                    "median": 0.91,
                    "sd": 0.08,
                    "p05": 0.79,
                    "p25": 0.86,
                    "p75": 0.98,
                    "p95": 1.05,
                    "minimum": 0.1,
                    "maximum": 3.0
                }
            ]
        }

        (out_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()