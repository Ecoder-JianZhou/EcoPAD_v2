import { api } from "../core/api.js";
import { MultiSelect } from "../components/MultiSelect.js";

import {
  SimPlot,
  ParamHistoryPlot,
  ParamHistGridPlot,
  VarInfoDot,
  selectCtrl,
  chip,
  toggle,
  parLabel,
} from "../components/plots.js";

const HIST_PARAM_MAX = 9;
const FORECAST_MODE_OPTIONS = ["forecast_with_da", "forecast_without_da"];
const AUTO_FORECAST_PARAM_MODE = "auto_forecast_with_da";

// ---------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------
function firstOrEmpty(arr) {
  return arr && arr.length ? arr[0] : "";
}

function firstAsArray(arr) {
  return arr && arr.length ? [arr[0]] : [];
}

function hasNonEmptySeries(series) {
  return (series || []).some((s) => (s.time || []).length > 0);
}

function hasObsPoints(data) {
  return Array.isArray(data?.points) && data.points.length > 0;
}

function parseSeriesKey(key) {
  if (!key) return { model: "", treatment: "" };

  if (key.includes("·")) {
    const parts = key.split("·").map((x) => x.trim());
    return { model: parts[0] || "", treatment: parts[1] || "" };
  }

  if (key.includes("||")) {
    const parts = key.split("||").map((x) => x.trim());
    return { model: parts[0] || "", treatment: parts[1] || "" };
  }

  return { model: key, treatment: "" };
}

function forecastModeLabel(v) {
  if (v === "forecast_with_da") return "Forecast with DA";
  if (v === "forecast_without_da") return "Forecast without DA";
  if (v === "auto_forecast_with_da") return "Auto Forecast with DA";
  if (v === "auto_forecast_without_da") return "Auto Forecast without DA";
  if (v === "simulation_with_da") return "Simulation with DA";
  if (v === "simulation_without_da" || v === "simulate") return "Simulation without DA";
  return v || "";
}

function expandForecastModesToRunOutputTypes(modes = []) {
  const out = [];
  const seen = new Set();

  function add(v) {
    const s = String(v || "").trim();
    if (!s || seen.has(s)) return;
    seen.add(s);
    out.push(s);
  }

  (modes || []).forEach((mode) => {
    if (mode === "forecast_with_da") {
      add("forecast_with_da");
      add("auto_forecast_with_da");
      return;
    }

    if (mode === "forecast_without_da") {
      add("forecast_without_da");
      add("auto_forecast_without_da");
      return;
    }

    add(mode);
  });

  return out;
}

function toSimData(data, outputType = "forecast_with_da") {
  const series = (data?.series || []).map((s) => {
    const parsed = parseSeriesKey(s.key || "");
    return {
      key: `${s.key || `${s.model || parsed.model}||${s.treatment || parsed.treatment}`}||${outputType}`,
      model: s.model || parsed.model,
      treatment: s.treatment || parsed.treatment,
      run_id: s.run_id || s.source_run_id || "",
      run_label: s.run_label || forecastModeLabel(outputType),
      output_type: outputType,
      series_type: outputType,
      time: s.time || [],
      mean: s.mean || [],
      q05: s.lo || s.q05 || [],
      q95: s.hi || s.q95 || [],
    };
  });

  return {
    units: data?.units || "",
    series,
  };
}

function normalizeObsData(raw) {
  if (!raw) return { points: [] };

  if (Array.isArray(raw?.points)) {
    return { points: raw.points };
  }

  const time = Array.isArray(raw?.time) ? raw.time : [];
  const value = Array.isArray(raw?.value) ? raw.value : [];
  const std = Array.isArray(raw?.std) ? raw.std : [];
  const treatment = raw?.treatment || "";
  const model = raw?.model || "";

  const n = Math.min(time.length, value.length);
  const points = [];

  for (let i = 0; i < n; i += 1) {
    points.push({
      model,
      time: time[i],
      value: value[i],
      std: std[i] ?? null,
      treatment,
    });
  }

  return { points };
}

// ---------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------
function pickRunRawTime(run) {
  if (!run) return "";
  return (
    run.finished_at ||
    run.started_at ||
    run.created_at ||
    run.updated_at ||
    run.time ||
    run.label ||
    ""
  );
}

function formatDateTimeStable(value) {
  if (!value) return "";
  const s = String(value).trim();
  if (!s) return "";

  const m = s.match(
    /^(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2})(?::(\d{2}))?(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/
  );
  if (m) {
    const date = m[1];
    const hhmm = m[2];
    const ss = m[3] ? `:${m[3]}` : ":00";
    return `${date} ${hhmm}${ss}`;
  }

  const m2 = s.match(/^(\d{4}-\d{2}-\d{2})$/);
  if (m2) return m2[1];

  return s;
}

function sortableTimestamp(value) {
  if (!value) return Number.NaN;
  const s = String(value).trim();
  if (!s) return Number.NaN;

  const isoLike = s.includes("T") ? s : s.replace(" ", "T");
  const ts = Date.parse(isoLike);
  return Number.isNaN(ts) ? Number.NaN : ts;
}

function formatDateTime(iso) {
  return formatDateTimeStable(iso);
}

function getRunDisplayTime(run) {
  return formatDateTimeStable(pickRunRawTime(run));
}

function buildRunTooltip(run) {
  if (!run) return "";
  return [
    `Time: ${getRunDisplayTime(run) || "—"}`,
    `Run ID: ${run.run_id || "—"}`,
    `User: ${run.username || run.user || run.created_by || "—"}`,
    `Task ID: ${run.scheduled_task_id ?? "—"}`,
    `Status: ${run.status || "—"}`,
    `Trigger: ${run.triggered_by || "—"}`,
    `Model: ${run.model_id || "—"}`,
    `Treatment: ${run.treatment || "—"}`,
    `Output type: ${run.output_type || run.series_type || "—"}`,
  ].join("\n");
}

function intersectArrays(arrays) {
  if (!arrays || arrays.length === 0) return [];
  if (arrays.length === 1) return [...arrays[0]];
  const [first, ...rest] = arrays;
  return first.filter((x) => rest.every((arr) => arr.includes(x)));
}

function toNumberArray(rows, paramId) {
  return (rows || [])
    .map((r) => Number(r?.[paramId]))
    .filter((v) => Number.isFinite(v));
}

function sameStringArray(a = [], b = []) {
  if (a === b) return true;
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (String(a[i]) !== String(b[i])) return false;
  }
  return true;
}

function normalizeRunOptions(rows = []) {
  const normalized = (rows || []).map((row) => {
    const rawTime = pickRunRawTime(row);
    const label = formatDateTimeStable(rawTime) || String(row?.run_id || "");
    return {
      ...row,
      _raw_time: rawTime,
      label,
      output_type: row?.output_type || row?.series_type || "",
      series_type: row?.output_type || row?.series_type || "",
    };
  });

  return [...normalized].sort((a, b) => {
    const ta = sortableTimestamp(a?._raw_time);
    const tb = sortableTimestamp(b?._raw_time);
    const aBad = Number.isNaN(ta);
    const bBad = Number.isNaN(tb);

    if (aBad && bBad) return String(b?.run_id || "").localeCompare(String(a?.run_id || ""));
    if (aBad) return 1;
    if (bBad) return -1;
    return tb - ta;
  });
}

function optionLabel(run) {
  if (!run) return "";
  const latest = run.is_latest_published ? " · latest" : "";
  const ot = run.output_type || run.series_type || "";
  const mode = ot ? ` · ${forecastModeLabel(ot)}` : "";
  return `${getRunDisplayTime(run) || run.run_id || ""}${mode}${latest}`;
}

function pickRunRows(options = [], selectedKeys = []) {
  const wanted = new Set((selectedKeys || []).map((x) => String(x)));
  return (options || []).filter((x) => {
    const key = `${x?.run_id || ""}||${x?.output_type || x?.series_type || ""}`;
    return wanted.has(key);
  });
}

function buildParamHistFromAccepted(results, params) {
  const byParam = new Map();

  function ensureParam(paramId) {
    if (!byParam.has(paramId)) {
      byParam.set(paramId, {
        key: paramId,
        param: paramId,
        groups: [],
      });
    }
    return byParam.get(paramId);
  }

  (results || []).forEach((item) => {
    (params || []).forEach((paramId) => {
      const values = toNumberArray(item.rows || [], paramId);
      if (values.length === 0) return;

      const bucket = ensureParam(paramId);
      bucket.groups.push({
        key: `${item.run_id}||${item.model}||${item.treatment}`,
        run_id: item.run_id,
        model: item.model,
        treatment: item.treatment,
        label: `${item.model} · ${item.treatment} · ${item.run_id}`,
        values,
      });
    });
  });

  return {
    hist: Array.from(byParam.values()),
  };
}

function normalizeHistoryForPlot(data, selectedModels = [], selectedTreatments = []) {
  const rows = Array.isArray(data?.series) ? data.series : [];
  if (rows.length === 0) return { series: [] };

  const fallbackModel = firstOrEmpty(selectedModels);
  const fallbackTreatment = firstOrEmpty(selectedTreatments);
  const grouped = new Map();

  function ensureGroup(model, treatment) {
    const key = `${model} · ${treatment}`;
    if (!grouped.has(key)) {
      grouped.set(key, {
        key,
        model,
        treatment,
        time: [],
        mean: [],
        q05: [],
        q95: [],
        run_ids: [],
      });
    }
    return grouped.get(key);
  }

  rows.forEach((row) => {
    const model = String(row?.model_id || row?.model || fallbackModel || "");
    const treatment = String(row?.treatment || fallbackTreatment || "");

    if (
      (row?.time || row?.data_time || row?.analysis_time || row?.run_time || row?.updated_at || row?.run_finished_at) &&
      row?.value !== undefined
    ) {
      const timeValue = String(
        row?.data_time ||
          row?.analysis_time ||
          row?.time ||
          row?.run_time ||
          row?.updated_at ||
          row?.run_finished_at ||
          row?.run_updated_at ||
          ""
      );
      const value = Number(row?.value);
      if (!Number.isFinite(value) || !timeValue) return;

      const item = ensureGroup(model, treatment);
      item.time.push(formatDateTimeStable(timeValue));
      item.mean.push(value);

      const lo = Number(row?.q05);
      const hi = Number(row?.q95);
      item.q05.push(Number.isFinite(lo) ? lo : null);
      item.q95.push(Number.isFinite(hi) ? hi : null);
      item.run_ids.push(String(row?.run_id || ""));
      return;
    }

    const times = Array.isArray(row?.run_time) ? row.run_time : [];
    const values = Array.isArray(row?.best) ? row.best : [];
    const q05 = Array.isArray(row?.q05) ? row.q05 : [];
    const q95 = Array.isArray(row?.q95) ? row.q95 : [];
    const runIds = Array.isArray(row?.run_ids) ? row.run_ids : [];

    if (times.length === 0 || values.length === 0) return;

    const item = ensureGroup(model, treatment);
    const n = Math.min(times.length, values.length);

    for (let i = 0; i < n; i += 1) {
      const value = Number(values[i]);
      const timeValue = String(times[i] || "");
      if (!Number.isFinite(value) || !timeValue) continue;

      item.time.push(formatDateTimeStable(timeValue));
      item.mean.push(value);

      const lo = Number(q05[i]);
      const hi = Number(q95[i]);
      item.q05.push(Number.isFinite(lo) ? lo : null);
      item.q95.push(Number.isFinite(hi) ? hi : null);
      item.run_ids.push(String(runIds[i] || ""));
    }
  });

  const series = Array.from(grouped.values())
    .map((item) => {
      const order = item.time
        .map((t, i) => ({ i, t, ts: sortableTimestamp(t) }))
        .sort((a, b) => {
          const aBad = Number.isNaN(a.ts);
          const bBad = Number.isNaN(b.ts);
          if (aBad && bBad) return a.i - b.i;
          if (aBad) return 1;
          if (bBad) return -1;
          return a.ts - b.ts;
        });

      return {
        ...item,
        time: order.map((x) => item.time[x.i]),
        mean: order.map((x) => item.mean[x.i]),
        q05: order.map((x) => item.q05[x.i]),
        q95: order.map((x) => item.q95[x.i]),
        run_ids: order.map((x) => item.run_ids[x.i]),
      };
    })
    .filter((item) => item.time.length > 0 && item.mean.length > 0);

  return { series };
}

// ---------------------------------------------------------------------
// Run select widgets
// ---------------------------------------------------------------------
function RunMultiSelect({ options = [], selected = [], onChange, size = 6 }) {
  const e = React.createElement;

  return e(
    "select",
    {
      multiple: true,
      size: Math.min(Math.max(size, 4), 10),
      value: selected,
      onChange: (ev) => {
        const values = Array.from(ev.target.selectedOptions).map((opt) => opt.value);
        onChange(values);
      },
    },
    (options || []).map((item) => {
      const key = `${item.run_id}||${item.output_type || item.series_type || ""}`;
      return e(
        "option",
        {
          key,
          value: key,
          title: buildRunTooltip(item),
        },
        optionLabel(item)
      );
    })
  );
}

function renderSelectedRunInline(e, rows = []) {
  if (!rows || rows.length === 0) return null;

  if (rows.length === 1) {
    const row = rows[0];
    return e(
      "div",
      {
        className: "muted",
        style: { marginTop: 6, whiteSpace: "pre-wrap" },
        title: buildRunTooltip(row),
      },
      `Selected: ${getRunDisplayTime(row) || row.run_id || "—"} · ${forecastModeLabel(
        row.output_type || row.series_type
      )} · ${row.status || "—"} · ${row.username || row.user || row.created_by || "unknown user"}`
    );
  }

  return e(
    "div",
    {
      className: "muted",
      style: { marginTop: 6 },
      title: rows.map((r) => buildRunTooltip(r)).join("\n\n"),
    },
    `${rows.length} runs selected`
  );
}

// ---------------------------------------------------------------------
// Inline parameter tooltip
// ---------------------------------------------------------------------
function InlineParamInfo({ paramId, infoMap, fallbackMap }) {
  const e = React.createElement;

  const info =
    (paramId && infoMap && infoMap[paramId]) ||
    (paramId && fallbackMap && fallbackMap[paramId]) ||
    null;

  if (!paramId || !info) return null;

  const symbol = info.symbol || paramId;
  const fullName = info.full || info.name || "";
  const title = fullName ? `${fullName} (${symbol})` : symbol;

  function row(label, value) {
    if (value === undefined || value === null || value === "") return null;
    return e(
      "div",
      { className: "param-info-row" },
      e("span", { className: "param-info-k" }, `${label}: `),
      e("span", { className: "param-info-v" }, String(value))
    );
  }

  return e(
    "span",
    { className: "param-info-inline tooltip-inline" },
    e(
      "button",
      {
        type: "button",
        className: "param-info-icon",
        tabIndex: 0,
        "aria-label": `Show parameter info for ${symbol}`,
      },
      "!"
    ),
    e(
      "span",
      { className: "param-info-tip" },
      e("div", { className: "title" }, title),
      info.description ? e("div", { className: "param-info-desc" }, info.description) : null,
      e(
        "div",
        { className: "param-info-grid" },
        row("ID", paramId),
        row("Symbol", symbol),
        row("Unit", info.unit),
        row("Default", info.default),
        row("Min", info.min ?? info.minimum),
        row("Max", info.max ?? info.maximum),
        row("Typical range", info.typical_range),
        row("Category", info.category),
        row("Group", info.group),
        row("Module", info.module),
        row("Dimension", info.dimension),
        row("Source", info.source)
      ),
      info.notes ? e("div", { className: "param-info-note" }, info.notes) : null
    )
  );
}

function renderHistogramParamTip(e, paramId, info) {
  if (!info) return null;

  const sym = info.symbol || paramId;
  const title = info.full ? `${info.full} (${sym})` : sym;

  function row(label, value) {
    if (value === undefined || value === null || value === "") return null;
    return e(
      "div",
      { className: "param-info-row" },
      e("span", { className: "param-info-k" }, `${label}: `),
      e("span", { className: "param-info-v" }, String(value))
    );
  }

  return e(
    "span",
    { className: "tip" },
    e("div", { className: "title" }, title),
    info.description ? e("div", { className: "param-info-desc" }, info.description) : null,
    e(
      "div",
      { className: "param-info-grid" },
      row("ID", paramId),
      row("Symbol", sym),
      row("Unit", info.unit),
      row("Default", info.default),
      row("Min", info.min ?? info.minimum),
      row("Max", info.max ?? info.maximum),
      row("Typical range", info.typical_range),
      row("Category", info.category),
      row("Group", info.group),
      row("Module", info.module),
      row("Dimension", info.dimension),
      row("Source", info.source)
    ),
    info.notes ? e("div", { className: "param-info-note" }, info.notes) : null
  );
}

// ---------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------
export function Forecast() {
  const e = React.createElement;

  const [sites, setSites] = React.useState([]);
  const [site, setSite] = React.useState("");
  const [meta, setMeta] = React.useState(null);
  const [view, setView] = React.useState("sim");

  const [sitesLoading, setSitesLoading] = React.useState(false);
  const [metaLoading, setMetaLoading] = React.useState(false);
  const [simLoading, setSimLoading] = React.useState(false);
  const [paramsLoading, setParamsLoading] = React.useState(false);
  const [summaryLoading, setSummaryLoading] = React.useState(false);
  const [obsLoading, setObsLoading] = React.useState(false);

  const [sitesError, setSitesError] = React.useState("");
  const [metaError, setMetaError] = React.useState("");
  const [simError, setSimError] = React.useState("");
  const [paramsError, setParamsError] = React.useState("");
  const [summaryError, setSummaryError] = React.useState("");

  const [varReg, setVarReg] = React.useState({});
  const [parReg, setParReg] = React.useState({});
  const [summary, setSummary] = React.useState(null);

  const [variable, setVariable] = React.useState("GPP");
  const [models, setModels] = React.useState([]);
  const [treatments, setTreatments] = React.useState([]);
  const [forecastModes, setForecastModes] = React.useState(["forecast_with_da"]);
  const [showObs, setShowObs] = React.useState(true);
  const [simData, setSimData] = React.useState(null);
  const [obsData, setObsData] = React.useState({ points: [] });

  const [simRunOptions, setSimRunOptions] = React.useState([]);
  const [simRuns, setSimRuns] = React.useState([]);

  const [pMetaByModel, setPMetaByModel] = React.useState({});
  const [pTab, setPTab] = React.useState("hist");
  const [pParam, setPParam] = React.useState("");
  const [pModels, setPModels] = React.useState([]);
  const [pTreatments, setPTreatments] = React.useState([]);
  const [pHistory, setPHistory] = React.useState(null);

  const [histRunOptions, setHistRunOptions] = React.useState([]);
  const [hRuns, setHRuns] = React.useState([]);
  const [hModels, setHModels] = React.useState([]);
  const [hTreatments, setHTreatments] = React.useState([]);
  const [hParams, setHParams] = React.useState([]);
  const [hData, setHData] = React.useState(null);

  React.useEffect(() => {
    let cancelled = false;

    if (window.EcoPAD?.VarRegistry?.load) {
      window.EcoPAD.VarRegistry.load()
        .then((j) => {
          if (!cancelled) setVarReg(j || {});
        })
        .catch(() => {
          if (!cancelled) setVarReg({});
        });
    }

    if (window.EcoPAD?.ParRegistry?.load) {
      window.EcoPAD.ParRegistry.load()
        .then((j) => {
          if (!cancelled) setParReg(j || {});
        })
        .catch(() => {
          if (!cancelled) setParReg({});
        });
    }

    return () => {
      cancelled = true;
    };
  }, []);

  React.useEffect(() => {
    let cancelled = false;
    setSitesLoading(true);
    setSitesError("");

    api.forecastSites()
      .then((j) => {
        if (cancelled) return;
        const s = j?.sites || [];
        setSites(s);
        setSite((prev) => prev || s[0] || "");
      })
      .catch(() => {
        if (cancelled) return;
        setSitesError("Failed to load sites.");
        setSites([]);
        setSite("");
      })
      .finally(() => {
        if (!cancelled) setSitesLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  React.useEffect(() => {
    if (!site) return;
    setMetaError("");
    setSimError("");
    setParamsError("");
    setSummaryError("");
    setSimData(null);
    setObsData({ points: [] });
    setSimRunOptions([]);
    setSimRuns([]);
    setPMetaByModel({});
    setPHistory(null);
    setHistRunOptions([]);
    setHRuns([]);
    setHData(null);
    setSummary(null);
  }, [site]);

  React.useEffect(() => {
    if (!site) return;

    let cancelled = false;
    setMetaLoading(true);
    setMetaError("");

    api.forecastMeta(site)
      .then((m) => {
        if (cancelled) return;

        const vars = m?.variables || [];
        const ms = m?.models || [];
        const ts = m?.treatments || [];

        setMeta(m || {});

        if (vars.length === 0) {
          setVariable("");
          setModels([]);
          setTreatments([]);
          setSimData({ series: [], units: "" });
          setObsData({ points: [] });
          return;
        }

        setVariable((prev) => (prev && vars.includes(prev) ? prev : vars[0]));
        setModels((prev) => {
          const keep = (prev || []).filter((x) => ms.includes(x));
          return keep.length ? keep : firstAsArray(ms);
        });
        setTreatments((prev) => {
          const keep = (prev || []).filter((x) => ts.includes(x));
          return keep.length ? keep : firstAsArray(ts);
        });
      })
      .catch(() => {
        if (cancelled) return;
        setMeta({});
        setVariable("");
        setModels([]);
        setTreatments([]);
        setSimData({ series: [], units: "" });
        setObsData({ points: [] });
        setMetaError("Failed to load metadata.");
      })
      .finally(() => {
        if (!cancelled) setMetaLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [site]);

  React.useEffect(() => {
    if (!site) return;

    let cancelled = false;
    setSummaryLoading(true);
    setSummaryError("");

    api.forecastSummary(site)
      .then((j) => {
        if (cancelled) return;
        setSummary(j || { site_id: site, latest_update: null, published_items: 0 });
      })
      .catch(() => {
        if (cancelled) return;
        setSummary({ site_id: site, latest_update: null, published_items: 0 });
        setSummaryError("Failed to load latest update info.");
      })
      .finally(() => {
        if (!cancelled) setSummaryLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [site]);

  React.useEffect(() => {
    if (!site || view !== "sim" || !variable) {
      setSimRunOptions([]);
      setSimRuns([]);
      return;
    }

    if (models.length === 0 || treatments.length === 0) {
      setSimRunOptions([]);
      setSimRuns([]);
      return;
    }

    let cancelled = false;
    const activeModes = forecastModes.length ? forecastModes : ["forecast_with_da"];
    const queryOutputTypes = expandForecastModesToRunOutputTypes(activeModes);

    Promise.all(
      queryOutputTypes.map((mode) =>
        api
          .forecastRuns(site, models.join(","), treatments.join(","), variable, "", null, mode, 200)
          .then((j) =>
            (j?.runs || []).map((r) => ({
              ...r,
              output_type: r.output_type || r.series_type || mode,
              series_type: r.output_type || r.series_type || mode,
            }))
          )
          .catch(() => [])
      )
    )
      .then((parts) => {
        if (cancelled) return;

        const merged = parts.flat();
        const seen = new Set();

        const rows = normalizeRunOptions(
          merged.filter((r) => {
            const key = `${r.run_id}||${r.model_id}||${r.treatment}||${r.output_type || r.series_type}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
          })
        );

        setSimRunOptions(rows);

        setSimRuns((prev) =>
          (prev || []).filter((x) =>
            rows.some((r) => `${r.run_id}||${r.output_type || r.series_type || ""}` === x)
          )
        );
      })
      .catch(() => {
        if (!cancelled) {
          setSimRunOptions([]);
          setSimRuns([]);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [site, view, variable, models, treatments, forecastModes]);

  React.useEffect(() => {
    if (!site || !variable) return;

    if (models.length === 0 || treatments.length === 0) {
      setSimData({ series: [], units: "" });
      setObsData({ points: [] });
      return;
    }

    let cancelled = false;
    setSimLoading(true);
    setSimError("");

    const activeModes = forecastModes.length ? forecastModes : ["forecast_with_da"];
    const selectedRunRows = pickRunRows(simRunOptions, simRuns);

    if (selectedRunRows.length === 0) {
      Promise.all(
        activeModes.map((mode) =>
          api
            .forecastData(site, variable, models.join(","), treatments.join(","), mode, false)
            .then((d) => ({ mode, data: d || null }))
            .catch(() => null)
        )
      )
        .then((results) => {
          if (cancelled) return;

          let unitsOut = "";
          const seriesOut = [];

          (results || []).filter(Boolean).forEach((item) => {
            const mode = item?.mode;
            const d = item?.data;
            if (!d) return;

            const normalized = toSimData(d, mode);
            if (normalized.units && !unitsOut) unitsOut = normalized.units;

            (normalized.series || []).forEach((s) => {
              seriesOut.push({
                ...s,
                key: `${s.key}||${mode}`,
                run_label: forecastModeLabel(mode),
                output_type: mode,
                series_type: mode,
              });
            });
          });

          setSimData({ units: unitsOut, series: seriesOut });
        })
        .catch(() => {
          if (cancelled) return;
          setSimError("Failed to load simulations.");
          setSimData({ series: [], units: "" });
        })
        .finally(() => {
          if (!cancelled) setSimLoading(false);
        });
    } else {
      Promise.all(
        selectedRunRows.map((row) =>
          api
            .forecastRunTimeseries(
              site,
              row.run_id,
              variable,
              row.model_id,
              row.treatment,
              row.output_type || row.series_type || "forecast_with_da"
            )
            .then((d) => ({ row, data: d || null }))
            .catch(() => null)
        )
      )
        .then((results) => {
          if (cancelled) return;

          let unitsOut = "";
          const seriesOut = [];

          (results || []).filter(Boolean).forEach((item) => {
            const siteData = item?.data;
            const row = item?.row;
            if (!siteData || !row) return;

            const rawSeries = Array.isArray(siteData?.series) ? siteData.series : [];
            const first = rawSeries[0];
            if (!first || typeof first !== "object") return;

            const siteUnits = String(siteData?.units || "");
            if (siteUnits && !unitsOut) unitsOut = siteUnits;

            const outputType = row.output_type || row.series_type || "";

            seriesOut.push({
              key: `${row.run_id}||${row.model_id}||${row.treatment}||${outputType}`,
              model: `${row.model_id} @ ${getRunDisplayTime(row)} · ${forecastModeLabel(outputType)}`,
              treatment: row.treatment,
              run_id: row.run_id,
              run_label: `${getRunDisplayTime(row)} · ${forecastModeLabel(outputType)}`,
              output_type: outputType,
              series_type: outputType,
              time: first.time || [],
              mean: first.mean || [],
              q05: first.lo || first.q05 || [],
              q95: first.hi || first.q95 || [],
            });
          });

          setSimData({ units: unitsOut, series: seriesOut });
        })
        .catch(() => {
          if (cancelled) return;
          setSimError("Failed to load selected run simulations.");
          setSimData({ series: [], units: "" });
        })
        .finally(() => {
          if (!cancelled) setSimLoading(false);
        });
    }

    setObsLoading(true);
    api.forecastObs(site, variable, models.join(","), treatments.join(","))
      .then((j) => {
        if (cancelled) return;
        setObsData(normalizeObsData(j));
      })
      .catch(() => {
        if (!cancelled) setObsData({ points: [] });
      })
      .finally(() => {
        if (!cancelled) setObsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [site, variable, models, treatments, forecastModes, simRunOptions, simRuns]);

  React.useEffect(() => {
    if (!site || view !== "params") return;

    const ms = meta?.models || [];
    const ts = meta?.treatments || [];

    setPModels((prev) => {
      const keep = (prev || []).filter((x) => ms.includes(x));
      return keep.length ? keep : firstAsArray(ms);
    });

    setPTreatments((prev) => {
      const keep = (prev || []).filter((x) => ts.includes(x));
      return keep.length ? keep : firstAsArray(ts);
    });

    setHModels((prev) => {
      const keep = (prev || []).filter((x) => ms.includes(x));
      return keep.length ? keep : firstAsArray(ms);
    });

    setHTreatments((prev) => {
      const keep = (prev || []).filter((x) => ts.includes(x));
      return keep.length ? keep : firstAsArray(ts);
    });
  }, [site, view, meta]);

  React.useEffect(() => {
    if (!site || view !== "params") return;

    if (!pModels || pModels.length === 0) {
      setPMetaByModel({});
      return;
    }

    let cancelled = false;
    setParamsLoading(true);
    setParamsError("");

    Promise.all(
      pModels.map((modelId) =>
        api.forecastParamsMeta(site, modelId).then((j) => ({
          modelId,
          data: j || null,
        }))
      )
    )
      .then((results) => {
        if (cancelled) return;
        const next = {};
        results.forEach((item) => {
          if (item?.modelId && item?.data) next[item.modelId] = item.data;
        });
        setPMetaByModel(next);
        setPHistory(null);
        setHData(null);
      })
      .catch(() => {
        if (cancelled) return;
        setPMetaByModel({});
        setParamsError("Failed to load parameter metadata.");
      })
      .finally(() => {
        if (!cancelled) setParamsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [site, view, pModels]);

  const selectedPMetas = React.useMemo(
    () => (pModels || []).map((m) => pMetaByModel?.[m]).filter(Boolean),
    [pModels, pMetaByModel]
  );

  const commonParams = React.useMemo(() => {
    const arrays = selectedPMetas.map((m) => m?.params || []).filter((arr) => arr.length > 0);
    return intersectArrays(arrays);
  }, [selectedPMetas]);

  const firstSelectedModel = firstOrEmpty(pModels);

  const activeParamInfoMap = React.useMemo(() => {
    if (!firstSelectedModel) return {};
    return pMetaByModel?.[firstSelectedModel]?.param_info || {};
  }, [firstSelectedModel, pMetaByModel]);

  React.useEffect(() => {
    if (view !== "params") return;

    const nextPParam =
      pParam && commonParams.includes(pParam) ? pParam : firstOrEmpty(commonParams);
    if (nextPParam !== pParam) setPParam(nextPParam);

    const nextHParams = (() => {
      const keep = (hParams || []).filter((x) => commonParams.includes(x));
      if (keep.length) return keep.slice(0, HIST_PARAM_MAX);
      return firstAsArray(commonParams);
    })();
    if (!sameStringArray(nextHParams, hParams)) setHParams(nextHParams);
  }, [view, commonParams, pParam, hParams]);

  React.useEffect(() => {
    if (view !== "params") return;
    if (pTab !== "hist") {
      setHistRunOptions([]);
      setHRuns([]);
      return;
    }

    const histTreatment = firstOrEmpty(hTreatments);
    if (!site || hModels.length === 0 || !histTreatment) {
      setHistRunOptions([]);
      setHRuns([]);
      return;
    }

    let cancelled = false;

    api.forecastRuns(
      site,
      hModels.join(","),
      histTreatment,
      variable || "GPP",
      "auto_forecast",
      null,
      AUTO_FORECAST_PARAM_MODE,
      200
    )
      .then((j) => {
        if (cancelled) return;

        const rows = normalizeRunOptions(
          (j?.runs || []).map((r) => ({
            ...r,
            output_type: r.output_type || r.series_type || AUTO_FORECAST_PARAM_MODE,
            series_type: r.output_type || r.series_type || AUTO_FORECAST_PARAM_MODE,
          }))
        );

        setHistRunOptions(rows);

        setHRuns((prev) => {
          const keep = (prev || []).filter((x) =>
            rows.some((r) => `${r.run_id}||${r.output_type || r.series_type || ""}` === x)
          );
          if (keep.length) return keep;

          const latestRow = rows.find((r) => r.is_latest_published);
          if (latestRow?.run_id) {
            return [`${latestRow.run_id}||${latestRow.output_type || latestRow.series_type || ""}`];
          }

          return firstAsArray(rows.map((r) => `${r.run_id}||${r.output_type || r.series_type || ""}`));
        });
      })
      .catch(() => {
        if (!cancelled) {
          setHistRunOptions([]);
          setHRuns([]);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [view, pTab, site, hModels, hTreatments, variable]);

  React.useEffect(() => {
    if (view !== "params") return;
    if (pTab !== "history") return;

    if (!pParam || pModels.length === 0 || pTreatments.length === 0) {
      setPHistory({ series: [] });
      return;
    }

    let cancelled = false;
    setParamsLoading(true);
    setParamsError("");

    api
      .forecastParamsHistory(
        site,
        pParam,
        pModels.join(","),
        pTreatments.join(","),
        variable || "GPP",
        AUTO_FORECAST_PARAM_MODE
      )
      .then((j) => {
        if (cancelled) return;
        setPHistory(normalizeHistoryForPlot(j, pModels, pTreatments));
      })
      .catch(() => {
        if (cancelled) return;
        setPHistory({ series: [] });
        setParamsError("Failed to load parameter history.");
      })
      .finally(() => {
        if (!cancelled) setParamsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [view, site, pTab, pParam, pModels, pTreatments, variable]);

  React.useEffect(() => {
    if (view !== "params") return;
    if (pTab !== "hist") return;

    const histTreatment = firstOrEmpty(hTreatments);
    if ((hRuns || []).length === 0 || hModels.length === 0 || !histTreatment || hParams.length === 0) {
      setHData({ hist: [] });
      return;
    }

    let cancelled = false;
    setParamsLoading(true);
    setParamsError("");

    const requests = [];
    (hRuns || []).forEach((runKey) => {
      const [runId, outputTypeRaw] = String(runKey || "").split("||");
      const outputType = outputTypeRaw || AUTO_FORECAST_PARAM_MODE;

      hModels.forEach((modelId) => {
        requests.push(
          api
            .forecastRunParametersAccepted(site, runId, modelId, histTreatment, outputType)
            .then((j) => ({
              run_id: runId,
              model: modelId,
              treatment: histTreatment,
              rows: j?.rows || [],
            }))
            .catch(() => null)
        );
      });
    });

    Promise.all(requests)
      .then((results) => {
        if (cancelled) return;
        const ok = (results || []).filter(Boolean);
        setHData(buildParamHistFromAccepted(ok, hParams.slice(0, HIST_PARAM_MAX)));
      })
      .catch(() => {
        if (cancelled) return;
        setHData({ hist: [] });
        setParamsError("Failed to load parameter histogram.");
      })
      .finally(() => {
        if (!cancelled) setParamsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [view, site, pTab, hRuns, hModels, hTreatments, hParams]);

  const obsAvailableForSelection = React.useMemo(() => {
    return hasObsPoints(obsData);
  }, [obsData]);

  const labels = meta?.treatment_labels || {};
  const showTrt = (k) => (labels && labels[k] ? labels[k] : k || "");

  const unitFromRegistry = varReg?.[variable]?.unit || "";
  const unitFromMeta = meta?.units?.[variable] || "";
  const units = unitFromRegistry || unitFromMeta || simData?.units || "";

  const latestUpdateText = summary?.latest_update ? formatDateTime(summary.latest_update) : "";
  const latestAutoUpdateText = summary?.latest_auto_forecast?.updated_at
    ? formatDateTime(summary.latest_auto_forecast.updated_at)
    : "";

  const hasSimOptions = (meta?.variables || []).length > 0;
  const hasSelection = site && variable && models.length > 0 && treatments.length > 0;
  const hasSeries = hasNonEmptySeries(simData?.series);
  const hasObs = showObs && obsAvailableForSelection;
  const hasAnyData = hasSeries || hasObs;
  const hasParamsData = pModels.length > 0 && commonParams.length > 0 && pTreatments.length > 0;

  const selectedSimRunRows = React.useMemo(
    () => pickRunRows(simRunOptions, simRuns),
    [simRunOptions, simRuns]
  );

  const selectedHistRunRows = React.useMemo(
    () => pickRunRows(histRunOptions, hRuns),
    [histRunOptions, hRuns]
  );

  return e(
    "div",
    { className: "forecast-layout" },

    e(
      "div",
      { className: "panel forecast-controls" },

      e("h2", null, "Forecast"),

      selectCtrl("Site", site, setSite, sites),

      e(
        "div",
        { className: "ctrl" },
        e("label", null, "View"),
        e(
          "div",
          { className: "chips" },
          chip("Simulations", view === "sim", () => setView("sim")),
          chip("Parameters", view === "params", () => setView("params"))
        )
      ),

      view === "sim"
        ? e(
            React.Fragment,
            null,
            e(
              "div",
              { className: "ctrl" },
              e("label", null, "Variable"),
              e(
                "div",
                { className: "info-wrap" },
                e(
                  "select",
                  {
                    value: variable,
                    onChange: (ev) => setVariable(ev.target.value),
                    disabled: metaLoading || !hasSimOptions,
                  },
                  (meta?.variables || []).map((v) => e("option", { key: v, value: v }, v))
                ),
                e(VarInfoDot, { varKey: variable, registry: varReg })
              )
            ),

            e(
              "div",
              { className: "ctrl" },
              e("label", null, "Models (multi)"),
              e(MultiSelect, {
                options: meta?.models || [],
                selected: models,
                onChange: setModels,
                max: 10,
              })
            ),

            e(
              "div",
              { className: "ctrl" },
              e("label", null, "Treatments (multi)"),
              e(MultiSelect, {
                options: meta?.treatments || [],
                selected: treatments,
                onChange: setTreatments,
                max: 20,
              })
            ),

            e(
              "div",
              { className: "ctrl" },
              e("label", null, "Forecast modes"),
              e(MultiSelect, {
                options: FORECAST_MODE_OPTIONS,
                selected: forecastModes,
                onChange: setForecastModes,
                max: 2,
              })
            ),

            e(
              "div",
              { className: "ctrl" },
              e(
                "label",
                {
                  style: {
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    gap: 10,
                  },
                },
                e("span", null, "Observation"),
                !obsLoading && !obsAvailableForSelection
                  ? e(
                      "span",
                      {
                        className: "muted",
                        style: {
                          fontSize: 12,
                          color: "#999",
                          whiteSpace: "nowrap",
                        },
                      },
                      "No data available"
                    )
                  : null
              ),
              e(
                "div",
                {
                  className: "chips",
                  style: {
                    opacity: obsLoading || obsAvailableForSelection ? 1 : 0.5,
                    pointerEvents: obsLoading || obsAvailableForSelection ? "auto" : "none",
                  },
                },
                chip("Show", (obsLoading || obsAvailableForSelection) && showObs, () => setShowObs(true)),
                chip("Hide", (obsLoading || obsAvailableForSelection) && !showObs, () => setShowObs(false))
              )
            ),

            e(
              "div",
              { className: "ctrl" },
              e("label", null, "Runs (multi, optional)"),
              e(RunMultiSelect, {
                options: simRunOptions,
                selected: simRuns,
                onChange: setSimRuns,
                size: 6,
              }),
              e(
                "div",
                { className: "muted", style: { marginTop: 6 } },
                simRuns.length === 0
                  ? "No run selected: showing latest published forecast."
                  : "Selected runs will override latest published view."
              ),
              renderSelectedRunInline(e, selectedSimRunRows)
            )
          )
        : e(
            React.Fragment,
            null,
            !hasParamsData && pModels.length === 0
              ? e("div", { className: "muted" }, "Select model(s) to view auto forecast parameters.")
              : e(
                  React.Fragment,
                  null,
                  e(
                    "div",
                    { className: "ctrl" },
                    e("label", null, "Parameters view"),
                    e(
                      "div",
                      { className: "chips" },
                      chip("Histogram", pTab === "hist", () => setPTab("hist")),
                      chip("History", pTab === "history", () => setPTab("history"))
                    )
                  ),

                  e(
                    "div",
                    { className: "ctrl" },
                    e("label", null, "Models (multi)"),
                    e(MultiSelect, {
                      options: meta?.models || [],
                      selected: pModels,
                      onChange: setPModels,
                      max: 10,
                    })
                  ),

                  pTab === "hist"
                    ? e(
                        React.Fragment,
                        null,
                        e(
                          "div",
                          { className: "ctrl" },
                          e("label", null, "Runs (multi)"),
                          e(RunMultiSelect, {
                            options: histRunOptions,
                            selected: hRuns,
                            onChange: setHRuns,
                            size: 6,
                          }),
                          renderSelectedRunInline(e, selectedHistRunRows)
                        ),
                        e(
                          "div",
                          { className: "ctrl" },
                          e("label", null, "Treatment (single for histogram)"),
                          e(
                            "select",
                            {
                              value: firstOrEmpty(hTreatments),
                              onChange: (ev) =>
                                setHTreatments(ev.target.value ? [ev.target.value] : []),
                              disabled: (meta?.treatments || []).length === 0,
                            },
                            (meta?.treatments || []).map((t) =>
                              e("option", { key: t, value: t }, t)
                            )
                          )
                        ),
                        e(
                          "div",
                          { className: "ctrl" },
                          e(
                            "label",
                            null,
                            `Parameters (shared across selected models, max ${HIST_PARAM_MAX})`
                          ),
                          e(
                            "div",
                            { className: "chips" },
                            commonParams.map((p) => {
                              const info = activeParamInfoMap?.[p] || parReg?.[p] || null;
                              const sym = info?.symbol || p;

                              return e(
                                "span",
                                { key: p, className: "tooltip-inline" },
                                e(
                                  "button",
                                  {
                                    type: "button",
                                    className: "chip" + (hParams.includes(p) ? " active" : ""),
                                    onClick: () => {
                                      setHParams((prev) => {
                                        const next = toggle(prev, p);
                                        return next.length > HIST_PARAM_MAX ? prev : next;
                                      });
                                    },
                                  },
                                  sym
                                ),
                                renderHistogramParamTip(e, p, info)
                              );
                            })
                          )
                        )
                      )
                    : e(
                        React.Fragment,
                        null,
                        e(
                          "div",
                          { className: "ctrl" },
                          e("label", null, "Treatments (multi)"),
                          e(MultiSelect, {
                            options: meta?.treatments || [],
                            selected: pTreatments,
                            onChange: setPTreatments,
                            max: 20,
                          })
                        ),
                        e(
                          "div",
                          { className: "ctrl" },
                          e("label", null, "Parameter (shared across selected models)"),
                          e(
                            "div",
                            { className: "param-select-wrap" },
                            e(
                              "div",
                              { className: "param-select-row" },
                              e(
                                "select",
                                {
                                  className: "param-select",
                                  value: pParam || "",
                                  onChange: (ev) => setPParam(ev.target.value),
                                  disabled: commonParams.length === 0,
                                },
                                commonParams.length > 0
                                  ? commonParams.map((k) =>
                                      e("option", { key: k, value: k }, parLabel(k, parReg))
                                    )
                                  : [e("option", { key: "", value: "" }, "No common parameter")]
                              ),
                              e(InlineParamInfo, {
                                paramId: pParam,
                                infoMap: activeParamInfoMap,
                                fallbackMap: parReg,
                              })
                            )
                          )
                        )
                      )
                )
          ),

      sitesLoading ? e("div", { className: "muted" }, "Loading sites...") : null,
      sitesError ? e("div", { className: "muted" }, sitesError) : null,
      metaLoading ? e("div", { className: "muted" }, "Loading metadata...") : null,
      metaError ? e("div", { className: "muted" }, metaError) : null,
      view === "sim" && simLoading ? e("div", { className: "muted" }, "Loading simulations...") : null,
      view === "sim" && obsLoading ? e("div", { className: "muted" }, "Loading observations...") : null,
      view === "sim" && simError ? e("div", { className: "muted" }, simError) : null,
      view === "params" && paramsLoading ? e("div", { className: "muted" }, "Loading parameters...") : null,
      view === "params" && paramsError ? e("div", { className: "muted" }, paramsError) : null,
      summaryError ? e("div", { className: "muted" }, summaryError) : null
    ),

    e(
      "div",
      { className: "panel forecast-view" },
      view === "sim"
        ? e(
            React.Fragment,
            null,
            e(
              "div",
              { className: "section-head" },
              e(
                "div",
                null,
                e(
                  "h2",
                  null,
                  variable
                    ? `${site} · ${variable}` + (!hasAnyData && hasSelection ? " · No Data Available" : "")
                    : `${site}`
                ),
                e("div", { className: "muted" }, variable && units ? `Units: ${units}` : ""),
                e(
                  "div",
                  { className: "muted", style: { marginTop: 6 } },
                  summaryLoading
                    ? "Loading latest update..."
                    : latestUpdateText
                      ? `Latest update: ${latestUpdateText}`
                      : latestAutoUpdateText
                        ? `Latest auto Forecast update: ${latestAutoUpdateText}`
                        : "No published update available yet."
                )
              )
            ),
            !hasSimOptions
              ? e("div", { className: "muted", style: { padding: "12px" } }, "No Data")
              : !hasSelection
                ? e(
                    "div",
                    { className: "muted", style: { padding: "12px" } },
                    "Select model(s) and treatment(s) to view simulations."
                  )
                : hasAnyData
                  ? e(SimPlot, {
                      simData: simData || { series: [], units: "" },
                      obsData: showObs && obsAvailableForSelection
                        ? (obsData || { points: [] })
                        : { points: [] },
                      units,
                      showTrt,
                    })
                  : simLoading || obsLoading
                    ? e("div", { className: "muted", style: { padding: "12px" } }, "Loading...")
                    : e("div", { className: "muted", style: { padding: "12px" } }, "No Data")
          )
        : !hasParamsData && pModels.length > 0
          ? e(
              "div",
              { className: "muted", style: { padding: "12px" } },
              "No common parameter across selected models, or no treatment selected."
            )
          : e(
              React.Fragment,
              null,
              pTab === "hist"
                ? e(
                    React.Fragment,
                    null,
                    e(
                      "div",
                      { className: "section-head" },
                      e(
                        "div",
                        null,
                        e("h2", null, `${site} · Histogram`),
                        e(
                          "div",
                          { className: "muted" },
                          (hRuns || []).length > 0 ? `${hRuns.length} run(s)` : "No run selected"
                        ),
                        e(
                          "div",
                          { className: "muted", style: { marginTop: 6 } },
                          summaryLoading
                            ? "Loading latest update..."
                            : latestUpdateText
                              ? `Latest update: ${latestUpdateText}`
                              : latestAutoUpdateText
                                ? `Latest auto Forecast update: ${latestAutoUpdateText}`
                                : "No published update available yet."
                        )
                      )
                    ),
                    e(ParamHistGridPlot, { data: hData || { hist: [] }, showTrt })
                  )
                : e(
                    React.Fragment,
                    null,
                    e(
                      "div",
                      { className: "section-head" },
                      e(
                        "div",
                        null,
                        e("h2", null, `${site} · Parameter history · ${pParam || ""}`),
                        e("div", { className: "muted" }, "Multi-run auto-forecast history"),
                        e(
                          "div",
                          { className: "muted", style: { marginTop: 6 } },
                          summaryLoading
                            ? "Loading latest update..."
                            : latestUpdateText
                              ? `Latest update: ${latestUpdateText}`
                              : latestAutoUpdateText
                                ? `Latest auto Forecast update: ${latestAutoUpdateText}`
                                : "No published update available yet."
                        )
                      )
                    ),
                    e(ParamHistoryPlot, { data: pHistory || { series: [] }, showTrt })
                  )
            )
    )
  );
}