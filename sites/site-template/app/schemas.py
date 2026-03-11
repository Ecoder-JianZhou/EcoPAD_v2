from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


TaskType = Literal["simulate", "forecast", "auto_forecast", "mcmc", "custom"]
TriggerType = Literal["manual", "scheduled", "system"]


# ---------------------------------------------------------------------
# Run request
# ---------------------------------------------------------------------
class RunRequest(BaseModel):
    run_id: str
    scheduled_task_id: int | None = None
    site_id: str
    model_id: str
    task_type: TaskType
    trigger_type: TriggerType = "manual"
    payload: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------
# Manifest / artifacts
# ---------------------------------------------------------------------
class ArtifactItem(BaseModel):
    artifact_type: str
    model_id: str = ""
    treatment: str = ""
    variable: str = ""
    rel_path: str = ""
    media_type: str = "application/json"
    reader: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class ForecastSourceRef(BaseModel):
    rel_path: str = ""
    media_type: str = "application/json"


class ForecastRegistryItem(BaseModel):
    model_id: str
    treatment: str
    variable: str
    data_path: str = ""
    source_ref: dict[str, Any] = Field(default_factory=dict)
    is_published: int = 1


class ManifestResponse(BaseModel):
    run_id: str
    scheduled_task_id: int | None = None
    site_id: str
    model_id: str
    task_type: str
    trigger_type: str = "manual"
    request: dict[str, Any] = Field(default_factory=dict)
    execution: dict[str, Any] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[ArtifactItem] = Field(default_factory=list)

    # New canonical field expected by Runner dispatcher
    forecast_registry: list[ForecastRegistryItem] = Field(default_factory=list)


# ---------------------------------------------------------------------
# Timeseries response
# ---------------------------------------------------------------------
class TimeSeriesPoint(BaseModel):
    date: str
    value: float


class TimeSeriesResponse(BaseModel):
    run_id: str
    variable: str
    model: str
    treatment: str
    unit: str = ""
    series: list[TimeSeriesPoint] = Field(default_factory=list)


# ---------------------------------------------------------------------
# Params meta
# ---------------------------------------------------------------------
class ParamMetaItem(BaseModel):
    id: str
    name: str = ""
    unit: str = ""
    default: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    desc: str = ""


class ParamsMetaResponse(BaseModel):
    site_id: str
    model_id: str
    params: list[str] = Field(default_factory=list)
    param_info: dict[str, dict[str, Any]] = Field(default_factory=dict)


# ---------------------------------------------------------------------
# Parameter summary
# ---------------------------------------------------------------------
class ParameterSummaryValue(BaseModel):
    id: str
    name: str = ""
    unit: str = ""
    default: float | None = None
    optimized: float | None = None
    map: float | None = None
    mean: float | None = None
    median: float | None = None
    sd: float | None = None
    p05: float | None = None
    p25: float | None = None
    p75: float | None = None
    p95: float | None = None
    minimum: float | None = None
    maximum: float | None = None


class ParameterSummaryResponse(BaseModel):
    model_id: str
    task_type: str
    parameter_estimate_method: str = "posterior_mean"
    parameters: list[ParameterSummaryValue] = Field(default_factory=list)