import { api } from "../core/api.js";
import { Modal } from "./Modal.js";
import { PlotSVG } from "./PlotSVG.js";

const SUPPORTED_OUTPUT_TYPES = [
  "simulation_without_da",
  "simulation_with_da",
  "forecast_with_da",
  "forecast_without_da",
  "auto_forecast_with_da",
  "auto_forecast_without_da",
];

function normalizeText(v) {
  return String(v || "").trim();
}

function normalizeOutputType(v) {
  const s = normalizeText(v);
  if (!s) return "";

  const mapping = {
    simulate: "simulation_without_da",
    simulation_without_da: "simulation_without_da",
    simulation_with_da: "simulation_with_da",
    forecast_with_da: "forecast_with_da",
    forecast_without_da: "forecast_without_da",
    auto_forecast_with_da: "auto_forecast_with_da",
    auto_forecast_without_da: "auto_forecast_without_da",
  };

  return mapping[s] || s;
}

function isNonEmptyObject(obj) {
  return !!obj && typeof obj === "object" && !Array.isArray(obj) && Object.keys(obj).length > 0;
}

function deriveRequestedOutputType(man) {
  const expected = man?.request?.expected_output_types;
  if (Array.isArray(expected) && expected.length > 0) {
    return normalizeOutputType(expected[0]);
  }

  const fromPayload = normalizeOutputType(man?.request?.payload?.series_type);
  if (SUPPORTED_OUTPUT_TYPES.includes(fromPayload)) return fromPayload;

  const fromTask = normalizeOutputType(man?.task_type || man?.request?.task);
  if (SUPPORTED_OUTPUT_TYPES.includes(fromTask)) return fromTask;

  return "";
}

function deriveModels(man) {
  const out = [];
  const seen = new Set();

  function add(v) {
    const s = normalizeText(v);
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

function deriveTreatments(man, model = "") {
  const out = [];
  const seen = new Set();

  function add(v) {
    const s = normalizeText(v);
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
    if (model && isNonEmptyObject(indexObj[model])) {
      Object.keys(indexObj[model]).forEach(add);
    } else {
      Object.values(indexObj).forEach((modelBlock) => {
        if (isNonEmptyObject(modelBlock)) {
          Object.keys(modelBlock).forEach(add);
        }
      });
    }
  }

  if (Array.isArray(man?.artifacts)) {
    man.artifacts.forEach((a) => {
      const am = normalizeText(a?.model_id);
      if (!model || am === model) add(a?.treatment);
    });
  }

  return out;
}

function deriveOutputTypes(man, model = "", treatment = "") {
  const requested = deriveRequestedOutputType(man);
  const out = [];
  const seen = new Set();

  function add(v) {
    const s = normalizeOutputType(v);
    if (!s || seen.has(s)) return;
    if (!SUPPORTED_OUTPUT_TYPES.includes(s)) return;
    seen.add(s);
    out.push(s);
  }

  const expected = man?.request?.expected_output_types;
  if (Array.isArray(expected)) {
    expected.forEach(add);
  }

  const indexObj = man?.outputs?.index;
  const treatmentBlock =
    indexObj && model && treatment
      ? indexObj?.[model]?.[treatment]
      : null;

  if (isNonEmptyObject(treatmentBlock)) {
    Object.entries(treatmentBlock).forEach(([outputType, outputBlock]) => {
      if (isNonEmptyObject(outputBlock)) {
        add(outputType);
      }
    });
  }

  if (Array.isArray(man?.artifacts)) {
    man.artifacts.forEach((a) => {
      if (normalizeText(a?.artifact_type) !== "timeseries") return;

      const am = normalizeText(a?.model_id);
      const at = normalizeText(a?.treatment);
      const aot = normalizeOutputType(a?.output_type || a?.series_type);
      const relPath = normalizeText(a?.rel_path);

      if ((!model || am === model) && (!treatment || at === treatment) && relPath) {
        add(aot);
      }
    });
  }

  if (requested && out.includes(requested)) {
    return [requested, ...out.filter((x) => x !== requested)];
  }

  return out;
}

function deriveVariables(man, model, treatment, outputType = "") {
  const out = [];
  const seen = new Set();

  function add(v) {
    const s = normalizeText(v);
    if (!s || seen.has(s)) return;
    seen.add(s);
    out.push(s);
  }

  const normalizedOutputType = normalizeOutputType(outputType);

  const indexObj = man?.outputs?.index;
  if (indexObj && typeof indexObj === "object" && model && treatment) {
    const treatmentBlock = indexObj?.[model]?.[treatment];

    if (isNonEmptyObject(treatmentBlock)) {
      if (normalizedOutputType) {
        const outputBlock = treatmentBlock?.[normalizedOutputType];
        if (isNonEmptyObject(outputBlock)) {
          Object.keys(outputBlock).forEach(add);
        }
      } else {
        Object.values(treatmentBlock).forEach((outputBlock) => {
          if (isNonEmptyObject(outputBlock)) {
            Object.keys(outputBlock).forEach(add);
          }
        });
      }
    }
  }

  if (Array.isArray(man?.artifacts)) {
    man.artifacts.forEach((a) => {
      if (normalizeText(a?.artifact_type) !== "timeseries") return;

      const am = normalizeText(a?.model_id);
      const at = normalizeText(a?.treatment);
      const aot = normalizeOutputType(a?.output_type || a?.series_type);
      const relPath = normalizeText(a?.rel_path);

      if (
        relPath &&
        (!model || am === model) &&
        (!treatment || at === treatment) &&
        (!normalizedOutputType || aot === normalizedOutputType)
      ) {
        add(a?.variable);
      }
    });
  }

  return out;
}

function preferredVariable(vars = []) {
  if (vars.includes("GPP")) return "GPP";
  return vars[0] || "";
}

function preferredOutputType(types = [], man = null) {
  const requested = deriveRequestedOutputType(man);

  if (requested && types.includes(requested)) return requested;
  if (types.includes("auto_forecast_with_da")) return "auto_forecast_with_da";
  if (types.includes("auto_forecast_without_da")) return "auto_forecast_without_da";
  if (types.includes("forecast_with_da")) return "forecast_with_da";
  if (types.includes("forecast_without_da")) return "forecast_without_da";
  if (types.includes("simulation_with_da")) return "simulation_with_da";
  if (types.includes("simulation_without_da")) return "simulation_without_da";
  return types[0] || "";
}

function triggerBrowserDownload(url) {
  const a = document.createElement("a");
  a.href = url;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

export function AccountJobViewerModal({
  auth,
  open,
  activeJob,
  manifest,
  result,
  loadingRun,
  variable,
  selModel,
  selTreatment,
  selOutputType,
  setVariable,
  setSelModel,
  setSelTreatment,
  setSelOutputType,
  reloadTimeseries,
  onClose,
}) {
  const e = React.createElement;
  const [downloadBusy, setDownloadBusy] = React.useState("");

  async function handleDownloadManifest() {
    if (!activeJob?.id) return;
    setDownloadBusy("manifest");
    try {
      const url = `/api/workflow/runs/${encodeURIComponent(activeJob.id)}/manifest`;
      triggerBrowserDownload(url);
    } finally {
      setTimeout(() => setDownloadBusy(""), 300);
    }
  }

  async function handleDownloadZip() {
    if (!activeJob?.id) return;
    setDownloadBusy("zip");
    try {
      const url = api.runDownloadUrl(activeJob.id, { bundle: true });
      triggerBrowserDownload(url);
    } finally {
      setTimeout(() => setDownloadBusy(""), 300);
    }
  }

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
          e(
            "div",
            {
              style: {
                display: "flex",
                gap: 8,
                alignItems: "center",
                flexWrap: "wrap",
              },
            },
            e("div", { className: "muted" }, `Status: ${activeJob.status || "-"}`),
            e(
              "button",
              {
                type: "button",
                className: "btn",
                onClick: handleDownloadManifest,
                disabled: downloadBusy === "manifest",
              },
              downloadBusy === "manifest" ? "Downloading..." : "Manifest"
            ),
            e(
              "button",
              {
                type: "button",
                className: "btn",
                onClick: handleDownloadZip,
                disabled: downloadBusy === "zip",
              },
              downloadBusy === "zip" ? "Downloading..." : "Download zip"
            )
          )
        ),

        (() => {
          const modelOptions = deriveModels(manifest);
          const treatmentOptions = deriveTreatments(manifest, selModel);
          const outputTypeOptions = deriveOutputTypes(manifest, selModel, selTreatment);
          const variableOptions = deriveVariables(manifest, selModel, selTreatment, selOutputType);

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
              { className: "ctrl", style: { minWidth: 240 } },
              e("label", null, "Model"),
              e(
                "select",
                {
                  value: selModel,
                  onChange: async (ev) => {
                    const nextModel = ev.target.value;

                    const nextTreatments = deriveTreatments(manifest, nextModel);
                    const nextTreatment = nextTreatments[0] || "";

                    const nextOutputTypes = deriveOutputTypes(manifest, nextModel, nextTreatment);
                    const nextOutputType = preferredOutputType(nextOutputTypes, manifest);

                    const nextVars = deriveVariables(
                      manifest,
                      nextModel,
                      nextTreatment,
                      nextOutputType
                    );
                    const nextVar = preferredVariable(nextVars);

                    setSelModel(nextModel);
                    setSelTreatment(nextTreatment);
                    setSelOutputType(nextOutputType);
                    setVariable(nextVar);

                    if (nextVar && nextOutputType) {
                      await reloadTimeseries({
                        model: nextModel,
                        treatment: nextTreatment,
                        output_type: nextOutputType,
                        variable: nextVar,
                      });
                    }
                  },
                },
                modelOptions.map((x) =>
                  e("option", { key: x, value: x }, x)
                )
              )
            ),

            e(
              "div",
              { className: "ctrl", style: { minWidth: 240 } },
              e("label", null, "Treatment"),
              e(
                "select",
                {
                  value: selTreatment,
                  onChange: async (ev) => {
                    const nextTreatment = ev.target.value;

                    const nextOutputTypes = deriveOutputTypes(manifest, selModel, nextTreatment);
                    const nextOutputType = preferredOutputType(nextOutputTypes, manifest);

                    const nextVars = deriveVariables(
                      manifest,
                      selModel,
                      nextTreatment,
                      nextOutputType
                    );
                    const nextVar = preferredVariable(nextVars);

                    setSelTreatment(nextTreatment);
                    setSelOutputType(nextOutputType);
                    setVariable(nextVar);

                    if (nextVar && nextOutputType) {
                      await reloadTimeseries({
                        treatment: nextTreatment,
                        output_type: nextOutputType,
                        variable: nextVar,
                      });
                    }
                  },
                },
                treatmentOptions.map((x) =>
                  e("option", { key: x, value: x }, x)
                )
              )
            ),

            outputTypeOptions.length > 1
              ? e(
                  "div",
                  { className: "ctrl", style: { minWidth: 260 } },
                  e("label", null, "Output type"),
                  e(
                    "select",
                    {
                      value: selOutputType,
                      onChange: async (ev) => {
                        const nextOutputType = ev.target.value;

                        const nextVars = deriveVariables(
                          manifest,
                          selModel,
                          selTreatment,
                          nextOutputType
                        );
                        const nextVar = preferredVariable(nextVars);

                        setSelOutputType(nextOutputType);
                        setVariable(nextVar);

                        if (nextVar) {
                          await reloadTimeseries({
                            output_type: nextOutputType,
                            variable: nextVar,
                          });
                        }
                      },
                    },
                    outputTypeOptions.map((x) =>
                      e("option", { key: x, value: x }, x)
                    )
                  )
                )
              : e(
                  "div",
                  { className: "ctrl", style: { minWidth: 260 } },
                  e("label", null, "Output type"),
                  e(
                    "input",
                    {
                      className: "wf-input",
                      value: selOutputType || "",
                      readOnly: true,
                    }
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

                    if (nextVar) {
                      await reloadTimeseries({
                        variable: nextVar,
                        output_type: selOutputType,
                      });
                    }
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
                onClick: () => {
                  if (!variable || !selOutputType) return;
                  reloadTimeseries({
                    variable,
                    model: selModel,
                    treatment: selTreatment,
                    output_type: selOutputType,
                  });
                },
                disabled: loadingRun || !variable || !selOutputType,
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
          `${variable || "-"} · ${selModel || "-"} · ${selTreatment || "-"} · ${selOutputType || "-"}`
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