/*
Plotly-based plotting utilities for Forecast and Run views.

Exports:
- SimPlot
- ParamHistoryPlot
- ParamHistGridPlot
- VarInfoDot
- ParInfoDot
- helpers: stableHash, colorFromKey, hexToRgb, toggle, chip, selectCtrl, parLabel
*/

export function stableHash(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

const PALETTE = [
  "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
  "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#0ea5e9", "#1d4ed8",
  "#f97316", "#16a34a", "#dc2626", "#a855f7", "#0f766e", "#ca8a04",
  "#db2777", "#334155", "#84cc16", "#0891b2", "#be123c", "#7c3aed",
];

export function colorFromKey(key) {
  return PALETTE[stableHash(key || "") % PALETTE.length];
}

export function hexToRgb(hex) {
  const h = (hex || "#000000").replace("#", "");
  const v = parseInt(h, 16);
  return {
    r: (v >> 16) & 255,
    g: (v >> 8) & 255,
    b: v & 255,
  };
}

export function toggle(arr, item) {
  const prev = Array.isArray(arr) ? arr : [];
  if (prev.includes(item)) return prev.filter((x) => x !== item);
  return [...prev, item];
}

export function chip(text, active, onClick) {
  const e = React.createElement;
  return e(
    "button",
    {
      type: "button",
      className: "chip" + (active ? " active" : ""),
      onClick,
      title: text,
    },
    text
  );
}

export function selectCtrl(label, value, onChange, options) {
  const e = React.createElement;
  const opts = Array.isArray(options) ? options : [];

  return e(
    "div",
    { className: "ctrl" },
    e("label", null, label),
    e(
      "select",
      {
        value: value || "",
        onChange: (ev) => onChange(ev.target.value),
      },
      opts.map((opt) => e("option", { key: opt, value: opt }, opt))
    )
  );
}

export function parLabel(key, paramInfo) {
  const info = paramInfo?.[key] || null;
  return info?.symbol || key;
}

export function VarInfoDot({ varKey, meta, registry }) {
  const e = React.createElement;

  const info =
    registry?.[varKey] ||
    meta?.variable_info?.[varKey] ||
    meta?.var_info?.[varKey] ||
    null;

  const unit =
    info?.unit ||
    meta?.units?.[varKey] ||
    registry?.[varKey]?.unit ||
    "";

  if (!info && !unit) return null;

  const title =
    info?.full ? `${info.full} (${varKey})` :
    info?.name ? `${info.name} (${varKey})` :
    varKey;

  const description = info?.description || info?.desc || "";
  const note = info?.note || info?.notes || "";

  return e(
    "span",
    { className: "info-tooltip" },
    e("span", { className: "info-dot", title }, "i"),
    e(
      "span",
      { className: "info-tip" },
      e("div", { className: "title" }, title),
      description ? e("div", null, description) : null,
      unit ? e("div", { className: "muted", style: { marginTop: 6 } }, `Unit: ${unit}`) : null,
      note ? e("div", { className: "note" }, note) : null
    )
  );
}

export function ParInfoDot({ parKey, paramInfo, registry }) {
  const e = React.createElement;
  const info = paramInfo?.[parKey] || registry?.[parKey] || null;

  if (!info) return null;

  const symbol = info.symbol || parKey;
  const title =
    info.full ? `${info.full} (${symbol})`
    : info.name ? `${info.name} (${symbol})`
    : symbol;

  const unit = info.unit ? `Unit: ${info.unit}` : "";
  const description = info.description || info.desc || "";
  const range =
    info.typical_range
      ? `Typical range: ${info.typical_range}`
      : ((info.minimum !== undefined || info.maximum !== undefined)
          ? `Range: ${info.minimum ?? "-"} ~ ${info.maximum ?? "-"}`
          : "");
  const note = info.notes || info.note || "";

  return e(
    "span",
    { className: "info-tooltip" },
    e("span", { className: "info-dot", title }, "i"),
    e(
      "span",
      { className: "info-tip" },
      e("div", { className: "title" }, title),
      description ? e("div", null, description) : null,
      unit ? e("div", { className: "muted", style: { marginTop: 6 } }, unit) : null,
      range ? e("div", { className: "muted", style: { marginTop: 6 } }, range) : null,
      note ? e("div", { className: "note" }, note) : null
    )
  );
}

export function SimPlot({ simData, obsData, units, showTrt }) {
  const e = React.createElement;
  const ref = React.useRef(null);

  React.useEffect(() => {
    const el = ref.current;
    if (!el || !window.Plotly) return;

    const series = simData?.series || [];
    const points = obsData?.points || [];

    if (!series.length && !points.length) {
      window.Plotly.purge(el);
      return;
    }

    const sorted = series.slice().sort((a, b) => {
      const modelCmp = (a.model || "").localeCompare(b.model || "");
      if (modelCmp !== 0) return modelCmp;
      return (a.treatment || "").localeCompare(b.treatment || "");
    });

    const traces = [];

    sorted.forEach((s) => {
      const model = s.model || "";
      const treatment = s.treatment || "";
      const key = `${model}||${treatment}`;
      const color = colorFromKey(key);
      const rgb = hexToRgb(color);

      const x = (s.time || []).map((t) => new Date(t));
      const y = s.mean || [];
      const lo = s.q05 || s.lo || [];
      const hi = s.q95 || s.hi || [];

      if (hi.length && lo.length) {
        traces.push({
          type: "scatter",
          mode: "lines",
          x,
          y: hi,
          line: { width: 0 },
          hoverinfo: "skip",
          showlegend: false,
        });

        traces.push({
          type: "scatter",
          mode: "lines",
          x,
          y: lo,
          line: { width: 0 },
          fill: "tonexty",
          fillcolor: `rgba(${rgb.r},${rgb.g},${rgb.b},0.18)`,
          hoverinfo: "skip",
          showlegend: false,
        });
      }

      traces.push({
        type: "scatter",
        mode: "lines",
        name: `${model} · ${showTrt(treatment)}`,
        x,
        y,
        line: { color, width: 2.6 },
        hovertemplate: "%{y:.4f}<extra>%{fullData.name}</extra>",
      });
    });

    points.forEach((p) => {
      traces.push({
        type: "scatter",
        mode: "markers",
        name: `Obs · ${showTrt(p.treatment)}`,
        x: (p.time || []).map((t) => new Date(t)),
        y: p.value || [],
        marker: { color: "#111111", size: 6, opacity: 0.9 },
        hovertemplate: "%{y:.4f}<extra>Obs</extra>",
        showlegend: false,
      });
    });

    const layout = {
      margin: { l: 70, r: 20, t: 10, b: 55 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(255,255,255,0.75)",
      xaxis: {
        title: { text: "Time" },
        showgrid: true,
        gridcolor: "rgba(11,18,32,0.08)",
      },
      yaxis: {
        title: { text: units || "Value" },
        showgrid: true,
        gridcolor: "rgba(11,18,32,0.08)",
      },
      legend: {
        orientation: "h",
        y: -0.25,
        x: 0,
        font: { size: 11 },
      },
      hovermode: "x unified",
    };

    window.Plotly.react(el, traces, layout, {
      responsive: true,
      scrollZoom: true,
    });
  }, [simData, obsData, units, showTrt]);

  return e(
    "div",
    { className: "plotly-wrap" },
    e("div", { ref, className: "plotly-figure" })
  );
}

export function ParamHistoryPlot({ data, showTrt }) {
  const e = React.createElement;
  const ref = React.useRef(null);

  React.useEffect(() => {
    const el = ref.current;
    if (!el || !window.Plotly) return;

    const series = data?.series || [];
    if (!series.length) {
      window.Plotly.purge(el);
      return;
    }

    const traces = [];

    series.forEach((s) => {
      const key = `${s.model || ""}||${s.treatment || ""}`;
      const color = colorFromKey(key);
      const rgb = hexToRgb(color);

      const rawTime = s.run_time || s.time || [];
      const rawY = s.best || s.mean || s.value || [];
      const lo = s.q05 || s.lo || [];
      const hi = s.q95 || s.hi || [];

      const x = rawTime.map((t) => {
        const d = new Date(t);
        return Number.isNaN(d.getTime()) ? t : d;
      });

      if (hi.length && lo.length && hi.length === rawY.length && lo.length === rawY.length) {
        traces.push({
          type: "scatter",
          mode: "lines",
          x,
          y: hi,
          line: { width: 0 },
          hoverinfo: "skip",
          showlegend: false,
        });

        traces.push({
          type: "scatter",
          mode: "lines",
          x,
          y: lo,
          line: { width: 0 },
          fill: "tonexty",
          fillcolor: `rgba(${rgb.r},${rgb.g},${rgb.b},0.18)`,
          hoverinfo: "skip",
          showlegend: false,
        });
      }

      traces.push({
        type: "scatter",
        mode: "lines+markers",
        name: `${s.model || ""} · ${showTrt(s.treatment || "")}`,
        x,
        y: rawY,
        line: { color, width: 2.6 },
        marker: { color, size: 6 },
        hovertemplate: "%{x}<br>%{y:.6g}<extra>%{fullData.name}</extra>",
      });
    });

    const layout = {
      margin: { l: 70, r: 20, t: 10, b: 55 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(255,255,255,0.75)",
      xaxis: {
        title: { text: "Run time" },
        showgrid: true,
        gridcolor: "rgba(11,18,32,0.08)",
      },
      yaxis: {
        title: { text: "Parameter value" },
        showgrid: true,
        gridcolor: "rgba(11,18,32,0.08)",
      },
      legend: {
        orientation: "h",
        y: -0.25,
        x: 0,
        font: { size: 11 },
      },
      hovermode: "closest",
    };

    window.Plotly.react(el, traces, layout, {
      responsive: true,
      scrollZoom: true,
    });
  }, [data, showTrt]);

  return e(
    "div",
    { className: "plotly-wrap" },
    e("div", { ref, className: "plotly-figure" })
  );
}

function normalizeHistItems(rawHist) {
  const hist = Array.isArray(rawHist) ? rawHist : [];

  if (hist.length === 0) return [];

  const hasNestedSeries = hist.some((item) => Array.isArray(item?.series));
  if (hasNestedSeries) {
    return hist;
  }

  const grouped = new Map();

  hist.forEach((item) => {
    const param = String(item?.param || "").trim();
    if (!param) return;

    if (!grouped.has(param)) {
      grouped.set(param, {
        key: param,
        param,
        series: [],
      });
    }

    grouped.get(param).series.push({
      model: item?.model || "",
      treatment: item?.treatment || "",
      values: Array.isArray(item?.values) ? item.values : [],
      run_id: item?.run_id || "",
    });
  });

  return Array.from(grouped.values());
}

export function ParamHistGridPlot({ data, showTrt }) {
  const e = React.createElement;
  const ref = React.useRef(null);

  React.useEffect(() => {
    const el = ref.current;
    if (!el || !window.Plotly) return;

    const items = normalizeHistItems(data?.hist).slice(0, 9);

    if (!items.length) {
      window.Plotly.purge(el);
      return;
    }

    const n = items.length;
    const cols = n <= 2 ? n : (n <= 4 ? 2 : 3);
    const rows = Math.ceil(n / cols);

    const gapX = 0.05;
    const gapY = 0.10;
    const cellW = (1 - gapX * (cols - 1)) / cols;
    const cellH = (1 - gapY * (rows - 1)) / rows;

    const traces = [];
    const layout = {
      margin: { l: 55, r: 20, t: 10, b: 60 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(255,255,255,0.75)",
      showlegend: true,
      legend: { orientation: "h", y: -0.18, x: 0, font: { size: 11 } },
      barmode: "overlay",
      hovermode: "closest",
      annotations: [],
    };

    items.forEach((item, idx) => {
      const row = Math.floor(idx / cols);
      const col = idx % cols;

      const x0 = col * (cellW + gapX);
      const x1 = x0 + cellW;

      const yTop = 1 - row * (cellH + gapY);
      const y0 = yTop - cellH;
      const y1 = yTop;

      const ax = idx + 1;
      const xa = ax === 1 ? "x" : `x${ax}`;
      const ya = ax === 1 ? "y" : `y${ax}`;

      const xAxisKey = ax === 1 ? "xaxis" : `xaxis${ax}`;
      const yAxisKey = ax === 1 ? "yaxis" : `yaxis${ax}`;

      layout[xAxisKey] = {
        domain: [x0, x1],
        anchor: ya,
        showgrid: true,
        gridcolor: "rgba(11,18,32,0.08)",
        zeroline: false,
      };

      layout[yAxisKey] = {
        domain: [y0, y1],
        anchor: xa,
        showgrid: true,
        gridcolor: "rgba(11,18,32,0.08)",
        zeroline: false,
      };

      layout.annotations.push({
        xref: "paper",
        yref: "paper",
        x: x0 + 0.01,
        y: y1 - 0.01,
        xanchor: "left",
        yanchor: "top",
        text: `<b>${item.param}</b>`,
        showarrow: false,
        font: { size: 12, color: "rgba(11,18,32,0.92)" },
      });

      (item.series || []).forEach((s) => {
        const key = `${s.model || ""}||${s.treatment || ""}||${s.run_id || ""}`;
        const color = colorFromKey(key);

        traces.push({
          type: "histogram",
          x: s.values || [],
          nbinsx: 24,
          xaxis: xa,
          yaxis: ya,
          name: s.run_id
            ? `${s.model} · ${showTrt(s.treatment)} · ${s.run_id}`
            : `${s.model} · ${showTrt(s.treatment)}`,
          marker: { color },
          opacity: 0.38,
        });
      });
    });

    window.Plotly.react(el, traces, layout, { responsive: true });
  }, [data, showTrt]);

  return e(
    "div",
    { className: "plotly-wrap" },
    e("div", { ref, className: "plotly-figure plotly-figure-tall" })
  );
}