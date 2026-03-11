import { Modal } from "./Modal.js";
import { PlotSVG } from "./PlotSVG.js";

function deriveModels(man) {
  const out = [];
  const seen = new Set();

  function add(v) {
    const s = String(v || "").trim();
    if (!s || seen.has(s)) return;
    seen.add(s);
    out.push(s);
  }

  if (Array.isArray(man?.request?.models)) {
    man.request.models.forEach(add);
  }
  if (man?.request?.model) {
    add(man.request.model);
  }

  const indexObj = man?.outputs?.index;
  if (indexObj && typeof indexObj === "object") {
    Object.keys(indexObj).forEach(add);
  }

  if (Array.isArray(man?.artifacts)) {
    man.artifacts.forEach((a) => add(a?.model_id));
  }

  if (man?.model_id) {
    add(man.model_id);
  }

  return out;
}

function deriveTreatments(man) {
  const out = [];
  const seen = new Set();

  function add(v) {
    const s = String(v || "").trim();
    if (!s || seen.has(s)) return;
    seen.add(s);
    out.push(s);
  }

  if (Array.isArray(man?.request?.treatments)) {
    man.request.treatments.forEach(add);
  }
  if (man?.request?.treatment) {
    add(man.request.treatment);
  }

  const indexObj = man?.outputs?.index;
  if (indexObj && typeof indexObj === "object") {
    Object.values(indexObj).forEach((modelBlock) => {
      if (modelBlock && typeof modelBlock === "object") {
        Object.keys(modelBlock).forEach(add);
      }
    });
  }

  if (Array.isArray(man?.artifacts)) {
    man.artifacts.forEach((a) => add(a?.treatment));
  }

  return out;
}

function deriveVariables(man, model, treatment) {
  const out = [];
  const seen = new Set();

  function add(v) {
    const s = String(v || "").trim();
    if (!s || seen.has(s)) return;
    seen.add(s);
    out.push(s);
  }

  if (man?.outputs?.index && model && treatment) {
    const vars = Object.keys(man.outputs.index?.[model]?.[treatment] || {});
    vars.forEach(add);
  }

  if (Array.isArray(man?.artifacts)) {
    man.artifacts.forEach((a) => {
      if (a?.artifact_type !== "timeseries") return;

      const am = String(a?.model_id || "").trim();
      const at = String(a?.treatment || "").trim();

      if ((!model || am === model) && (!treatment || at === treatment)) {
        add(a?.variable);
      }
    });
  }

  if (man?.outputs?.timeseries && typeof man.outputs.timeseries === "object") {
    Object.keys(man.outputs.timeseries).forEach(add);
  }

  return out;
}

export function AccountJobViewerModal({
  open,
  activeJob,
  manifest,
  result,
  loadingRun,
  variable,
  selModel,
  selTreatment,
  setVariable,
  setSelModel,
  setSelTreatment,
  reloadTimeseries,
  onClose,
}) {
  const e = React.createElement;

  return e(
    Modal,
    {
      open,
      title: activeJob ? `Run ${activeJob.id}` : "Run",
      onClose,
      width: 1100,
    },

    !activeJob ? null : e(
      "div",
      null,

      e(
        "div",
        { className: "card", style: { marginBottom: 12 } },
        e(
          "div",
          { className: "section-head" },
          e("h3", null, "Run viewer"),
          e("div", { className: "muted" }, `Status: ${activeJob.status || "-"}`)
        ),

        (() => {
          const modelOptions = deriveModels(manifest);
          const treatmentOptions = deriveTreatments(manifest);

          let variableOptions = deriveVariables(manifest, selModel, selTreatment);
          if (!variableOptions.length) variableOptions = ["GPP"];

          return e(
            "div",
            {
              style: {
                display: "flex",
                gap: 12,
                alignItems: "center",
                flexWrap: "wrap",
              },
            },

            e(
              "div",
              { className: "ctrl", style: { minWidth: 260 } },
              e("label", null, "Model"),
              e(
                "select",
                {
                  value: selModel,
                  onChange: async (ev) => {
                    const nextModel = ev.target.value;
                    setSelModel(nextModel);

                    const nextTreatmentOptions = deriveTreatments(manifest);
                    const nextTreatment =
                      nextTreatmentOptions.includes(selTreatment)
                        ? selTreatment
                        : (nextTreatmentOptions[0] || "");

                    if (nextTreatment !== selTreatment) {
                      setSelTreatment(nextTreatment);
                    }

                    const vars = deriveVariables(manifest, nextModel, nextTreatment);
                    const nextVar = vars.includes("GPP") ? "GPP" : (vars[0] || "GPP");
                    setVariable(nextVar);

                    await reloadTimeseries({
                      model: nextModel,
                      treatment: nextTreatment,
                      variable: nextVar,
                    });
                  },
                },
                modelOptions.map((x) =>
                  e("option", { key: x, value: x }, x)
                )
              )
            ),

            e(
              "div",
              { className: "ctrl", style: { minWidth: 260 } },
              e("label", null, "Treatment"),
              e(
                "select",
                {
                  value: selTreatment,
                  onChange: async (ev) => {
                    const nextTreatment = ev.target.value;
                    setSelTreatment(nextTreatment);

                    const vars = deriveVariables(manifest, selModel, nextTreatment);
                    const nextVar = vars.includes("GPP") ? "GPP" : (vars[0] || "GPP");
                    setVariable(nextVar);

                    await reloadTimeseries({
                      treatment: nextTreatment,
                      variable: nextVar,
                    });
                  },
                },
                treatmentOptions.map((x) =>
                  e("option", { key: x, value: x }, x)
                )
              )
            ),

            e(
              "div",
              { className: "ctrl", style: { minWidth: 220 } },
              e("label", null, "Variable"),
              e(
                "select",
                {
                  value: variable,
                  onChange: async (ev) => {
                    const nextVar = ev.target.value;
                    setVariable(nextVar);
                    await reloadTimeseries({ variable: nextVar });
                  },
                },
                variableOptions.map((x) =>
                  e("option", { key: x, value: x }, x)
                )
              )
            ),

            e(
              "button",
              {
                type: "button",
                className: "btn",
                onClick: () => reloadTimeseries({}),
                disabled: loadingRun,
              },
              loadingRun ? "Loading..." : "Refresh"
            )
          );
        })()
      ),

      e(
        "div",
        { className: "card", style: { marginBottom: 12 } },
        e(
          "div",
          { className: "muted", style: { marginBottom: 6 } },
          `${variable || "-"} · ${selModel || "-"} · ${selTreatment || "-"}`
        ),
        (result && result.series && result.series.length)
          ? e(PlotSVG, {
              x: result.series[0].time || [],
              y: result.series[0].mean || [],
              points: false,
            })
          : e("div", { className: "muted" }, "No timeseries data (or run not done).")
      ),

      e(
        "details",
        { className: "card" },
        e("summary", { className: "muted" }, "Show raw manifest.json"),
        e(
          "pre",
          {
            className: "wf-pre",
            style: { maxHeight: 320, overflow: "auto" },
          },
          manifest ? JSON.stringify(manifest, null, 2) : "(no manifest)"
        )
      )
    )
  );
}