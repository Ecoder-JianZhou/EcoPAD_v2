from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


TaskType = Literal[
    "simulate",               # legacy compatibility
    "simulation_without_da",  # canonical
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
    "auto_forecast",
    "custom",
]

TriggerType = Literal["manual", "scheduled", "system"]

SeriesType = Literal[
    "simulate",               # legacy compatibility
    "simulation_without_da",  # canonical
    "simulation_with_da",
    "forecast_with_da",
    "forecast_without_da",
    "auto_forecast_with_da",
    "auto_forecast_without_da",
]


# ---------------------------------------------------------------------
# Run request
# ---------------------------------------------------------------------
class RunRequest(BaseModel):
    """
    Runner -> Site run request payload.
    """

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
    output_type: str = ""
    series_type: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class ForecastSourceRef(BaseModel):
    rel_path: str = ""
    media_type: str = "application/json"
    output_type: str = ""
    series_type: str = ""


class ForecastRegistryItem(BaseModel):
    model_id: str
    treatment: str
    variable: str
    output_type: str = ""
    series_type: str = "forecast_with_da"
    data_path: str = ""
    obs_path: str = ""
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
    forecast_registry: list[ForecastRegistryItem] = Field(default_factory=list)


# ---------------------------------------------------------------------
# Timeseries response
# ---------------------------------------------------------------------
class TimeSeriesSeriesItem(BaseModel):
    time: list[Any] = Field(default_factory=list)
    mean: list[Any] = Field(default_factory=list)
    lo: list[Any] = Field(default_factory=list)
    hi: list[Any] = Field(default_factory=list)


class TimeSeriesResponse(BaseModel):
    run_id: str
    variable: str
    model: str
    treatment: str
    output_type: str = ""
    series_type: str = "forecast_with_da"
    units: str = ""
    series: list[TimeSeriesSeriesItem] = Field(default_factory=list)


# ---------------------------------------------------------------------
# Observation response
# ---------------------------------------------------------------------
class ObservationSeriesItem(BaseModel):
    treatment: str = ""
    time: list[Any] = Field(default_factory=list)
    value: list[Any] = Field(default_factory=list)
    std: list[Any] = Field(default_factory=list)
    unit: str = ""
    time_resolution: str = ""


class ObservationResponse(BaseModel):
    variable: str
    points: list[ObservationSeriesItem] = Field(default_factory=list)


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
    output_type: str = ""
    series_type: str = ""
    parameter_estimate_method: str = "posterior_mean"
    parameters: list[ParameterSummaryValue] = Field(default_factory=list)