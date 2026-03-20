import { AppCtx } from "../core/context.js";
import { api } from "../core/api.js";
import { Modal } from "../components/Modal.js";
import { MultiSelect } from "../components/MultiSelect.js";
import { PlotSVG } from "../components/PlotSVG.js";

export function Workflow() {
  const e = React.createElement;
  const { auth, navigate } = React.useContext(AppCtx);

  // ------------------------------------------------------------
  // Auth guard
  // ------------------------------------------------------------
  React.useEffect(() => {
    if (auth.status === "ready" && !auth.user) navigate("Login");
  }, [auth, navigate]);

  // ------------------------------------------------------------
  // Global workflow metadata
  // ------------------------------------------------------------
  const [meta, setMeta] = React.useState(null);
  const [metaLoading, setMetaLoading] = React.useState(false);

  // ------------------------------------------------------------
  // Site / model / task state
  // ------------------------------------------------------------
  const [site, setSite] = React.useState("");
  const [siteMeta, setSiteMeta] = React.useState(null);

  const [model, setModel] = React.useState("");
  const [treatments, setTreatments] = React.useState([]);
  const [task, setTask] = React.useState("simulation_without_da");

  const [jobName, setJobName] = React.useState("");
  const [notes, setNotes] = React.useState("");

  // ------------------------------------------------------------
  // Workflow permissions
  // ------------------------------------------------------------
  const [wfPerms, setWfPerms] = React.useState(null);
  const [wfPermLoading, setWfPermLoading] = React.useState(false);

  // ------------------------------------------------------------
  // Site info modal
  // ------------------------------------------------------------
  const [siteOpen, setSiteOpen] = React.useState(false);
  const [siteInfo, setSiteInfo] = React.useState(null);
  const [siteLoading, setSiteLoading] = React.useState(false);

  // ------------------------------------------------------------
  // Forcing preview in site modal
  // ------------------------------------------------------------
  const [forcingVars, setForcingVars] = React.useState(["temperature"]);
  const [forcingData, setForcingData] = React.useState(null);

  // ------------------------------------------------------------
  // Parameter metadata and editor state
  // ------------------------------------------------------------
  const [pMeta, setPMeta] = React.useState(null);
  const [paramLoading, setParamLoading] = React.useState(false);

  const [pVals, setPVals] = React.useState({});
  const [pErrs, setPErrs] = React.useState({});

  const [pQuery, setPQuery] = React.useState("");
  const [pSelected, setPSelected] = React.useState(null);

  const [pRowDense, setPRowDense] = React.useState(true);
  const [pFilter, setPFilter] = React.useState("all");

  const listRef = React.useRef(null);
  const [scrollTop, setScrollTop] = React.useState(0);
  const [viewportH, setViewportH] = React.useState(420);

  // ------------------------------------------------------------
  // Submission state
  // ------------------------------------------------------------
  const [submitLoading, setSubmitLoading] = React.useState(false);

  // ------------------------------------------------------------
  // Auto forecast config
  // ------------------------------------------------------------
  const [runMode, setRunMode] = React.useState("once"); // once | schedule
  const [runOnceAfterCreate, setRunOnceAfterCreate] = React.useState(true);
  const [scheduleCron, setScheduleCron] = React.useState("0 0 * * *");
  const [selectedCronPreset, setSelectedCronPreset] = React.useState("0 0 * * *");
  const [autoForecastWithoutDA, setAutoForecastWithoutDA] = React.useState(true);

  // ------------------------------------------------------------
  // Small helpers
  // ------------------------------------------------------------
  function safeNum(v) {
    if (v === "" || v === null || v === undefined) return undefined;
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  }

  function buildDefaultParamValues(infoObj) {
    const nextVals = {};
    for (const pid of Object.keys(infoObj || {})) {
      const d = infoObj[pid]?.default;
      if (d !== undefined && d !== null && d !== "") {
        const n = Number(d);
        nextVals[pid] = Number.isFinite(n) ? n : d;
      }
    }
    return nextVals;
  }

  function validateParamValue(raw, info) {
    const v = raw === "" ? "" : Number(raw);
    if (raw === "") return "";
    if (!Number.isFinite(v)) return "Invalid number";

    const min = info?.minimum;
    const max = info?.maximum;

    if (min !== null && min !== undefined && min !== "" && v < Number(min)) {
      return `Below min (${min})`;
    }
    if (max !== null && max !== undefined && max !== "" && v > Number(max)) {
      return `Above max (${max})`;
    }
    return "";
  }

  function isParamChanged(pid, info) {
    const cur = safeNum(pVals[pid]);
    const def = safeNum(info?.default);
    if (cur === undefined && def === undefined) return false;
    return cur !== def;
  }

  function resetAllParams() {
    const infoObj = pMeta?.param_info || {};
    setPVals(buildDefaultParamValues(infoObj));
    setPErrs({});
  }

  function resetSingleParam(pid, info) {
    const d = info?.default;
    const nextVal =
      d !== undefined && d !== null && d !== "" && Number.isFinite(Number(d))
        ? Number(d)
        : d ?? "";

    setPVals((prev) => ({ ...prev, [pid]: nextVal }));
    setPErrs((prev) => ({ ...prev, [pid]: "" }));
  }

  function clearParameterState() {
    setPMeta(null);
    setPVals({});
    setPErrs({});
    setPSelected(null);
    setPQuery("");
    setPFilter("all");
    setScrollTop(0);
  }

  function normalizePerms(raw) {
    const input = raw || {};
    const out = {};
    Object.keys(input).forEach((siteId) => {
      const row = input[siteId] || {};
      out[siteId] = {
        can_auto_forecast: !!row.can_auto_forecast,
      };
    });
    return out;
  }

  function parseCronPreset(preset) {
    setScheduleCron(preset);
    setSelectedCronPreset(preset);
  }

  function cronLabel(expr) {
    if (expr === "*/5 * * * *") return "Every 5 minutes";
    if (expr === "0 * * * *") return "Every hour";
    if (expr === "0 0 * * *") return "Every day at 00:00";
    if (expr === "0 6 * * *") return "Every day at 06:00";
    if (expr === "0 0 * * 1") return "Every Monday";
    if (expr === "0 0 */14 * *") return "Every 2 weeks";
    return "Custom cron";
  }

  function preferredInitialSite(siteList) {
    if (!Array.isArray(siteList) || siteList.length === 0) return "";
    const nonTemplate = siteList.find((s) => s !== "Example");
    return nonTemplate || siteList[0] || "";
  }

  function handleSiteChange(nextSite) {
    // Clear all site-dependent state immediately in the same event turn.
    // This prevents a transient invalid state like:
    // new site + old model.
    setSite(nextSite);
    setSiteMeta(null);
    setModel("");
    setTreatments([]);
    setTask("simulation_without_da");
    clearParameterState();
    setParamLoading(false);
  }

  // ------------------------------------------------------------
  // Derived state
  // ------------------------------------------------------------
  const normalizedSitePerms = React.useMemo(() => {
    return normalizePerms(wfPerms?.site_permissions || {});
  }, [wfPerms]);

  const accessibleSites = React.useMemo(() => {
    return Array.isArray(meta?.sites) ? meta.sites : [];
  }, [meta]);

  const canAutoForecastForSite = React.useMemo(() => {
    if (auth.user?.role === "superuser") return true;
    return !!normalizedSitePerms?.[site]?.can_auto_forecast;
  }, [normalizedSitePerms, site, auth.user]);

  const availableTreatments = React.useMemo(() => {
    return Array.isArray(siteMeta?.treatments) ? siteMeta.treatments : [];
  }, [siteMeta]);

  const availableModels = React.useMemo(() => {
    return Array.isArray(siteMeta?.models) ? siteMeta.models : [];
  }, [siteMeta]);

  const availableTasks = React.useMemo(() => {
    if (Array.isArray(siteMeta?.tasks) && siteMeta.tasks.length > 0) return siteMeta.tasks;
    return [
      "simulation_without_da",
      "simulation_with_da",
      "forecast_with_da",
      "forecast_without_da",
      "auto_forecast",
    ];
  }, [siteMeta]);

  const taskOptions = React.useMemo(() => {
    const all = [
      { key: "simulation_without_da", label: "Simulation without DA" },
      { key: "simulation_with_da", label: "Simulation with DA" },
      { key: "forecast_with_da", label: "Forecast with DA" },
      { key: "forecast_without_da", label: "Forecast without DA" },
      { key: "auto_forecast", label: "Auto Forecast" },
    ];

    return all
      .filter((t) => availableTasks.includes(t.key))
      .map((t) => {
        if (t.key === "auto_forecast") {
          return {
            ...t,
            label: canAutoForecastForSite ? t.label : "Auto Forecast (no permission)",
            disabled: !canAutoForecastForSite,
          };
        }
        return { ...t, disabled: false };
      });
  }, [availableTasks, canAutoForecastForSite]);

  const paramInfo = pMeta?.param_info || {};
  const allParamIds = Object.keys(paramInfo);

  const changedParamCount = React.useMemo(() => {
    return allParamIds.filter((pid) => isParamChanged(pid, paramInfo[pid] || {})).length;
  }, [allParamIds, paramInfo, pVals]);

  const invalidParamCount = React.useMemo(() => {
    return Object.values(pErrs).filter(Boolean).length;
  }, [pErrs]);

  const hasParamError = invalidParamCount > 0;
  const finalJobName = jobName || `${site} · ${model} · ${task}`;
  const notesPreview = (notes || "").trim();

  const selectedSummary = [
    { k: "Site", v: site || "-" },
    { k: "Model", v: model || "-" },
    { k: "Task", v: task || "-" },
    ...(task === "auto_forecast"
      ? [
          { k: "Forecast with DA", v: "Default" },
          { k: "Also run without DA", v: autoForecastWithoutDA ? "Yes" : "No" },
        ]
      : []),
    {
      k: "Execution mode",
      v: task === "auto_forecast" ? (runMode === "schedule" ? "Schedule" : "Run once") : "Run once",
    },
    ...(task === "auto_forecast" && runMode === "schedule"
      ? [
          { k: "Run once now", v: runOnceAfterCreate ? "Yes" : "No" },
          { k: "Cron", v: scheduleCron || "-" },
          { k: "Cron meaning", v: cronLabel(scheduleCron) },
        ]
      : []),
    { k: "Treatments", v: `${treatments.length} selected` },
    { k: "Site access", v: site ? "Allowed" : "No site selected" },
    { k: "Auto Forecast permission", v: canAutoForecastForSite ? "Allowed" : "Not allowed" },
    { k: "Parameters changed", v: String(changedParamCount) },
    { k: "Parameters invalid", v: String(invalidParamCount) },
    { k: "Job name", v: finalJobName || "-" },
  ];

  // ------------------------------------------------------------
  // Load global workflow metadata
  // ------------------------------------------------------------
  React.useEffect(() => {
    if (auth.status !== "ready" || !auth.user) return;

    setMetaLoading(true);
    api.workflowMeta()
      .then((m) => setMeta(m || null))
      .finally(() => setMetaLoading(false));
  }, [auth.status, auth.user]);

  // ------------------------------------------------------------
  // Load workflow permissions
  // ------------------------------------------------------------
  React.useEffect(() => {
    if (auth.status !== "ready" || !auth.user || !auth.token) return;

    setWfPermLoading(true);
    api.workflowPermissionsMe(auth.token)
      .then((j) => setWfPerms(j || { role: auth.user.role, site_permissions: {} }))
      .catch(() => setWfPerms({ role: auth.user.role, site_permissions: {} }))
      .finally(() => setWfPermLoading(false));
  }, [auth.status, auth.user, auth.token]);

  // ------------------------------------------------------------
  // Initialize selected site
  // Prefer a non-template site if possible
  // ------------------------------------------------------------
  React.useEffect(() => {
    if (auth.status !== "ready" || !auth.user) return;
    if (!meta) return;
    if (wfPermLoading) return;

    const sites = accessibleSites;
    const firstSite = preferredInitialSite(sites);

    setSite((prev) => {
      if (prev && sites.includes(prev)) return prev;
      return firstSite;
    });

    if (!firstSite) {
      setSiteMeta(null);
      setModel("");
      setTreatments([]);
      setTask("simulation_without_da");
      clearParameterState();
      setParamLoading(false);
    }
  }, [auth.status, auth.user, meta, accessibleSites, wfPermLoading]);

  // ------------------------------------------------------------
  // Load site-specific metadata
  // Site switch should rebuild the whole workflow context:
  // siteMeta -> model -> task -> treatments -> parameters
  // ------------------------------------------------------------
  React.useEffect(() => {
    if (!auth.user || !site) {
      setSiteMeta(null);
      setModel("");
      setTreatments([]);
      setTask("simulation_without_da");
      clearParameterState();
      setParamLoading(false);
      return;
    }

    let alive = true;

    // Clear current site-specific state before loading new site metadata
    setSiteMeta(null);
    setModel("");
    setTreatments([]);
    setTask("simulation_without_da");
    clearParameterState();
    setParamLoading(false);

    api.workflowSiteMeta(site)
      .then((m) => {
        if (!alive) return;

        const sm = m || null;
        setSiteMeta(sm);

        const siteModels = Array.isArray(sm?.models) ? sm.models : [];
        const siteTreatments = Array.isArray(sm?.treatments) ? sm.treatments : [];
        const siteTasks =
          Array.isArray(sm?.tasks) && sm.tasks.length > 0
            ? sm.tasks
            : [
                "simulation_without_da",
                "simulation_with_da",
                "forecast_with_da",
                "forecast_without_da",
                "auto_forecast",
              ];

        const nextTask = siteTasks.includes("simulation_without_da")
          ? "simulation_without_da"
          : siteTasks[0] || "";

        setModel(siteModels[0] || "");
        setTreatments(siteTreatments.length > 0 ? [siteTreatments[0]] : []);
        setTask(nextTask);
      })
      .catch(() => {
        if (!alive) return;
        setSiteMeta(null);
        setModel("");
        setTreatments([]);
        setTask("simulation_without_da");
        clearParameterState();
        setParamLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [auth.user, site]);

  // ------------------------------------------------------------
  // If auto forecast is not allowed for this site, fall back safely
  // ------------------------------------------------------------
  React.useEffect(() => {
    if (task === "auto_forecast" && !canAutoForecastForSite) {
      const fallbackTask = availableTasks.includes("simulation_without_da")
        ? "simulation_without_da"
        : availableTasks[0] || "";
      setTask(fallbackTask);
      setRunMode("once");
    }
  }, [task, canAutoForecastForSite, availableTasks]);

  // ------------------------------------------------------------
  // Auto forecast schedule mode only applies to auto forecast
  // ------------------------------------------------------------
  React.useEffect(() => {
    if (task !== "auto_forecast") {
      setRunMode("once");
    }
  }, [task]);

  // ------------------------------------------------------------
  // Virtualized parameter list viewport observer
  // ------------------------------------------------------------
  React.useEffect(() => {
    const el = listRef.current;
    if (!el) return;

    const ro = new ResizeObserver(() => {
      setViewportH(el.clientHeight || 420);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // ------------------------------------------------------------
  // Load parameter metadata only when the current model is valid
  // for the current site. This prevents stale cross-site requests.
  // ------------------------------------------------------------
  React.useEffect(() => {
    if (!auth.user) return;

    if (!site || !siteMeta || !model) {
      clearParameterState();
      setParamLoading(false);
      return;
    }

    const siteModels = Array.isArray(siteMeta?.models) ? siteMeta.models : [];

    if (!siteModels.includes(model)) {
      clearParameterState();
      setParamLoading(false);
      return;
    }

    let alive = true;
    setParamLoading(true);

    api.workflowParamsMeta(site, model)
      .then((j) => {
        if (!alive) return;

        setPMeta(j || null);

        const info = j && j.param_info ? j.param_info : {};
        setPVals(buildDefaultParamValues(info));
        setPErrs({});
        setPSelected(null);
        setScrollTop(0);
      })
      .catch(() => {
        if (!alive) return;
        clearParameterState();
      })
      .finally(() => {
        if (!alive) return;
        setParamLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [auth.user, site, siteMeta, model]);

  // ------------------------------------------------------------
  // Site modal helpers
  // ------------------------------------------------------------
  async function loadForcing(siteId, varsArr) {
    const vars = varsArr.join(",");
    const d = await api.siteForcing(siteId, vars);
    setForcingData(d);
  }

  async function openSiteInfo() {
    if (!site) return;

    setSiteOpen(true);
    setSiteLoading(true);
    setSiteInfo(null);
    setForcingData(null);

    try {
      const m = await api.siteMeta(site);
      setSiteInfo(m);
      await loadForcing(site, forcingVars);
    } catch (ex) {
      setSiteInfo({ error: ex.message || "Failed to load site info." });
    } finally {
      setSiteLoading(false);
    }
  }

  // ------------------------------------------------------------
  // Submit workflow
  // ------------------------------------------------------------
  async function submit() {
    if (!auth.user) {
      navigate("Login");
      return;
    }

    if (!site || !model || treatments.length === 0) {
      alert("Please select a site, a model, and at least one treatment.");
      return;
    }

    const currentSiteModels = Array.isArray(siteMeta?.models) ? siteMeta.models : [];
    if (!currentSiteModels.includes(model)) {
      alert("The selected model does not belong to the current site. Please reselect the site or model.");
      return;
    }

    if (task === "auto_forecast" && !canAutoForecastForSite) {
      alert(`You do not have Auto Forecast permission for site ${site}.`);
      return;
    }

    if (hasParamError) {
      alert("Some parameter values are out of range. Please fix them before submitting.");
      return;
    }

    setSubmitLoading(true);
    try {
      if (task === "auto_forecast" && runMode === "schedule") {
        await api.schedulerCreateTask(auth.token, {
          site_id: site,
          model_id: model,
          cron_expr: scheduleCron,
          enabled: 1,
          payload: {
            name: finalJobName,
            treatments,
            parameters: pVals,
            da: {
              auto_forecast_without_da: autoForecastWithoutDA,
            },
            notes,
          },
        });

        if (runOnceAfterCreate) {
          await api.workflowSubmit(auth.token, {
            site,
            name: `${finalJobName} · initial run`,
            models: [model],
            treatments,
            task: "auto_forecast",
            parameters: pVals,
            da: {
              auto_forecast_without_da: autoForecastWithoutDA,
            },
            notes,
            submitted_by: auth.user?.username || "",
            user_id: auth.user?.id ?? null,
          });
        }

        alert(
          runOnceAfterCreate
            ? "Auto Forecast schedule created and initial run submitted."
            : "Auto Forecast schedule created."
        );
      } else {
        const payload = {
          site,
          name: finalJobName,
          models: [model],
          treatments,
          task,
          parameters: pVals,
          da: {},
          notes,
          submitted_by: auth.user?.username || "",
          user_id: auth.user?.id ?? null,
        };

        if (task === "auto_forecast") {
          payload.da = {
            auto_forecast_without_da: autoForecastWithoutDA,
          };
        }

        await api.workflowSubmit(auth.token, payload);
        alert("Job submitted.");
      }

      setNotes("");
      setJobName("");
      navigate("Account");
    } catch (ex) {
      alert(
        ex.message ||
          (task === "auto_forecast" && runMode === "schedule"
            ? "Failed to create schedule."
            : "Failed to submit job.")
      );
    } finally {
      setSubmitLoading(false);
    }
  }

  // ------------------------------------------------------------
  // Small rendering helper
  // ------------------------------------------------------------
  function renderSectionTitle(title, subtitle) {
    return e(
      "div",
      { className: "section-head" },
      e("h3", null, title),
      e("div", { className: "muted" }, subtitle || "")
    );
  }

  // ------------------------------------------------------------
  // Render
  // ------------------------------------------------------------
  return e(
    "div",
    { className: "wf-layout" },

    // ============================================================
    // Left panel
    // ============================================================
    e(
      "div",
      { className: "panel wf-left" },

      e(
        "div",
        { className: "section-head" },
        e("h2", null, "Custom Workflow"),
        e(
          "div",
          { className: "muted" },
          metaLoading
            ? "Loading workflow metadata..."
            : wfPermLoading
              ? "Loading workflow permissions..."
              : "Configure and submit a model job."
        )
      ),

      // ----------------------------------------------------------
      // Basic setup
      // ----------------------------------------------------------
      e(
        "div",
        { className: "card wf-card" },
        renderSectionTitle("Basic setup", "Choose site, model, task, and job name."),

        e(
          "div",
          { className: "wf-row" },
          e("label", null, "Site"),
          e(
            "div",
            { className: "wf-site-row" },
            e(
              "select",
              {
                value: site,
                onChange: (ev) => handleSiteChange(ev.target.value),
                disabled: metaLoading || accessibleSites.length === 0,
              },
              accessibleSites.length > 0
                ? accessibleSites.map((s) => e("option", { key: s, value: s }, s))
                : [e("option", { key: "", value: "" }, "No site available")]
            ),
            e(
              "button",
              {
                type: "button",
                className: "icon-btn",
                title: siteMeta?.site_name || siteMeta?.name || "View site information",
                onClick: openSiteInfo,
                disabled: !site,
              },
              "!"
            )
          )
        ),

        e(
          "div",
          { className: "wf-row" },
          e("label", null, "Model"),
          e(
            "select",
            {
              value: model,
              onChange: (ev) => setModel(ev.target.value),
              disabled: metaLoading || availableModels.length === 0,
            },
            availableModels.length > 0
              ? availableModels.map((m) => e("option", { key: m, value: m }, m))
              : [e("option", { key: "", value: "" }, "No model available")]
          )
        ),

        e(
          "div",
          { className: "wf-row" },
          e("label", null, "Task"),
          e(
            "select",
            {
              value: task,
              onChange: (ev) => setTask(ev.target.value),
              disabled: metaLoading || wfPermLoading || taskOptions.length === 0,
            },
            taskOptions.length > 0
              ? taskOptions.map((t) =>
                  e(
                    "option",
                    {
                      key: t.key,
                      value: t.key,
                      disabled: !!t.disabled,
                    },
                    t.label
                  )
                )
              : [e("option", { key: "", value: "" }, "No task available")]
          )
        ),

        !canAutoForecastForSite
          ? e(
              "div",
              {
                className: "muted",
                style: { marginTop: 8, color: "#b26a00" },
              },
              `Auto Forecast is not enabled for your account on site ${site || "-"}.`
            )
          : task === "auto_forecast"
            ? e(
                "div",
                {
                  className: "muted",
                  style: { marginTop: 8, color: "#1a7f37" },
                },
                `Auto Forecast is enabled for site ${site}.`
              )
            : null,

        task === "auto_forecast"
          ? e(
              "div",
              {
                style: {
                  marginTop: 12,
                  padding: 12,
                  border: "1px solid rgba(0,0,0,0.08)",
                  borderRadius: 10,
                  background: "rgba(0,0,0,0.02)",
                },
              },

              e("div", { style: { fontWeight: 700, marginBottom: 8 } }, "Auto Forecast settings"),

              e(
                "div",
                {
                  style: {
                    marginBottom: 12,
                    padding: "10px 12px",
                    borderRadius: 10,
                    background: "rgba(255,255,255,0.65)",
                    border: "1px solid rgba(0,0,0,0.06)",
                    fontSize: 14,
                    lineHeight: 1.45,
                  },
                },
                "Default run uses data assimilation to optimize parameters for forecasting."
              ),

              e(
                "label",
                {
                  style: {
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 10,
                    padding: "10px 12px",
                    border: "1px solid rgba(0,0,0,0.08)",
                    borderRadius: 10,
                    background: "#fff",
                    cursor: "pointer",
                    marginBottom: 0,
                    width: "100%",
                    boxSizing: "border-box",
                  },
                },
                e("input", {
                  type: "checkbox",
                  style: {
                    flex: "0 0 auto",
                    width: 16,
                    height: 16,
                    margin: 0,
                  },
                  checked: autoForecastWithoutDA,
                  onChange: (ev) => setAutoForecastWithoutDA(ev.target.checked),
                }),
                e(
                  "div",
                  {
                    style: {
                      lineHeight: 1.25,
                      minWidth: 0,
                      flex: "1 1 auto",
                    },
                  },
                  e(
                    "div",
                    {
                      style: {
                        fontWeight: 600,
                        fontSize: 13,
                        whiteSpace: "normal",
                        wordBreak: "break-word",
                      },
                    },
                    "Also run forecast without DA"
                  ),
                  e(
                    "div",
                    {
                      className: "muted",
                      style: { fontSize: 11, marginTop: 2 },
                    },
                    "Optional comparison output"
                  )
                )
              ),

              e(
                "div",
                {
                  style: {
                    display: "flex",
                    gap: 10,
                    flexWrap: "wrap",
                    alignItems: "stretch",
                    marginTop: 16,
                  },
                },

                e(
                  "label",
                  {
                    style: {
                      display: "inline-flex",
                      alignItems: "flex-start",
                      gap: 8,
                      padding: "8px 12px",
                      border:
                        runMode === "once"
                          ? "1px solid rgba(44,120,255,0.45)"
                          : "1px solid rgba(0,0,0,0.08)",
                      borderRadius: 999,
                      background: runMode === "once" ? "rgba(44,120,255,0.06)" : "#fff",
                      cursor: "pointer",
                      marginBottom: 0,
                      whiteSpace: "nowrap",
                    },
                  },
                  e("input", {
                    type: "radio",
                    name: "run-mode",
                    checked: runMode === "once",
                    onChange: () => setRunMode("once"),
                    style: { marginTop: 2 },
                  }),
                  e(
                    "div",
                    { style: { lineHeight: 1.2 } },
                    e("div", { style: { fontWeight: 700, fontSize: 14 } }, "Run once"),
                    e(
                      "div",
                      { className: "muted", style: { fontSize: 12, marginTop: 2 } },
                      "Submit immediately"
                    )
                  )
                ),

                e(
                  "label",
                  {
                    style: {
                      display: "inline-flex",
                      alignItems: "flex-start",
                      gap: 8,
                      padding: "8px 12px",
                      border:
                        runMode === "schedule"
                          ? "1px solid rgba(44,120,255,0.45)"
                          : "1px solid rgba(0,0,0,0.08)",
                      borderRadius: 999,
                      background: runMode === "schedule" ? "rgba(44,120,255,0.06)" : "#fff",
                      cursor: "pointer",
                      marginBottom: 0,
                      whiteSpace: "nowrap",
                    },
                  },
                  e("input", {
                    type: "radio",
                    name: "run-mode",
                    checked: runMode === "schedule",
                    onChange: () => setRunMode("schedule"),
                    style: { marginTop: 2 },
                  }),
                  e(
                    "div",
                    { style: { lineHeight: 1.2 } },
                    e("div", { style: { fontWeight: 700, fontSize: 14 } }, "Schedule"),
                    e(
                      "div",
                      { className: "muted", style: { fontSize: 12, marginTop: 2 } },
                      "Create recurring task"
                    )
                  )
                )
              ),

              runMode === "schedule"
                ? e(
                    "div",
                    { style: { marginTop: 14 } },

                    e(
                      "div",
                      {
                        style: {
                          display: "grid",
                          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
                          gap: 12,
                        },
                      },

                      e(
                        "div",
                        { className: "ctrl" },
                        e("label", null, "Initial execution"),
                        e(
                          "label",
                          {
                            style: {
                              display: "flex",
                              alignItems: "center",
                              gap: 10,
                              marginBottom: 0,
                              minHeight: 38,
                              width: "100%",
                              boxSizing: "border-box",
                              padding: "0 12px",
                              border: "1px solid rgba(0,0,0,0.08)",
                              borderRadius: 8,
                              background: "#fff",
                              cursor: "pointer",
                            },
                          },
                          e("input", {
                            type: "checkbox",
                            checked: runOnceAfterCreate,
                            onChange: (ev) => setRunOnceAfterCreate(ev.target.checked),
                            style: {
                              margin: 0,
                              width: 16,
                              height: 16,
                              flex: "0 0 auto",
                            },
                          }),
                          e(
                            "span",
                            {
                              style: {
                                fontSize: 13,
                                lineHeight: 1.3,
                              },
                            },
                            runOnceAfterCreate
                              ? "Run once immediately"
                              : "Wait until next scheduled time"
                          )
                        )
                      ),

                      e(
                        "div",
                        { className: "ctrl" },
                        e("label", null, "Cron expression"),
                        e("input", {
                          className: "wf-input",
                          value: scheduleCron,
                          onChange: (ev) => {
                            setScheduleCron(ev.target.value);
                            setSelectedCronPreset("");
                          },
                          placeholder: "e.g. 0 0 * * *",
                          style: {
                            minHeight: 38,
                            boxSizing: "border-box",
                          },
                        }),
                        e(
                          "div",
                          {
                            className: "muted",
                            style: { fontSize: 11, marginTop: 4, lineHeight: 1.35 },
                          },
                          "Format: minute hour day-of-month month day-of-week (e.g. 0 0 * * * runs daily at midnight)"
                        )
                      )
                    ),

                    e(
                      "div",
                      { style: { marginTop: 12 } },
                      e("div", { className: "muted", style: { marginBottom: 6 } }, "Quick presets"),
                      e(
                        "div",
                        { className: "chips" },

                        e(
                          "button",
                          {
                            type: "button",
                            className: `chip ${selectedCronPreset === "*/5 * * * *" ? "active" : ""}`,
                            onClick: () => parseCronPreset("*/5 * * * *"),
                          },
                          "Every 5 min"
                        ),

                        e(
                          "button",
                          {
                            type: "button",
                            className: `chip ${selectedCronPreset === "0 * * * *" ? "active" : ""}`,
                            onClick: () => parseCronPreset("0 * * * *"),
                          },
                          "Hourly"
                        ),

                        e(
                          "button",
                          {
                            type: "button",
                            className: `chip ${selectedCronPreset === "0 0 * * *" ? "active" : ""}`,
                            onClick: () => parseCronPreset("0 0 * * *"),
                          },
                          "Daily 00:00"
                        ),

                        e(
                          "button",
                          {
                            type: "button",
                            className: `chip ${selectedCronPreset === "0 6 * * *" ? "active" : ""}`,
                            onClick: () => parseCronPreset("0 6 * * *"),
                          },
                          "Daily 06:00"
                        ),

                        e(
                          "button",
                          {
                            type: "button",
                            className: `chip ${selectedCronPreset === "0 0 * * 1" ? "active" : ""}`,
                            onClick: () => parseCronPreset("0 0 * * 1"),
                          },
                          "Weekly"
                        ),

                        e(
                          "button",
                          {
                            type: "button",
                            className: `chip ${selectedCronPreset === "0 0 */14 * *" ? "active" : ""}`,
                            onClick: () => parseCronPreset("0 0 */14 * *"),
                          },
                          "Every 2 weeks"
                        )
                      )
                    ),

                    e(
                      "div",
                      {
                        className: "muted",
                        style: { marginTop: 10, fontSize: 13 },
                      },
                      `Cron meaning: ${cronLabel(scheduleCron)}`
                    )
                  )
                : null
            )
          : null,

        e(
          "div",
          { className: "wf-row" },
          e("label", null, "Job name"),
          e("input", {
            className: "wf-input",
            value: jobName,
            onChange: (ev) => setJobName(ev.target.value),
            placeholder: "e.g., test vcmax update",
          })
        )
      ),

      // ----------------------------------------------------------
      // Parameters
      // ----------------------------------------------------------
      e(
        "div",
        { className: "card wf-card" },

        e(
          "div",
          { className: "section-head", style: { alignItems: "center" } },
          e(
            "div",
            null,
            e("h3", null, "Parameters"),
            e("div", { className: "muted" }, "")
          )
        ),

        (() => {
          const infoObj = pMeta?.param_info || {};
          const allIds = Object.keys(infoObj);

          if (paramLoading) {
            return e("div", { className: "muted" }, "Loading parameter metadata...");
          }

          if (!pMeta || !pMeta.param_info) {
            return e("div", { className: "muted" }, "No parameter meta for this site/model.");
          }

          const invalidIdsAll = allIds.filter((pid) => !!pErrs[pid]);
          const invalidCount = invalidIdsAll.length;

          const changedIdsAll = allIds.filter((pid) => {
            const info = infoObj[pid] || {};
            return isParamChanged(pid, info);
          });
          const changedCount = changedIdsAll.length;

          let baseIds = allIds;
          if (pFilter === "changed") baseIds = changedIdsAll;
          if (pFilter === "invalid") baseIds = invalidIdsAll;

          const q = (pQuery || "").trim().toLowerCase();
          const ids = !q
            ? baseIds
            : baseIds.filter((pid) => {
                const info = infoObj[pid] || {};
                const hay = `${pid} ${info.name || ""} ${info.desc || ""}`.toLowerCase();
                return hay.includes(q);
              });

          const rowH = pRowDense ? 34 : 44;
          const overscan = 10;

          const total = ids.length;
          const totalH = total * rowH;

          const startIdx = Math.max(0, Math.floor(scrollTop / rowH) - overscan);
          const endIdx = Math.min(total, Math.ceil((scrollTop + viewportH) / rowH) + overscan);
          const visibleIds = ids.slice(startIdx, endIdx);

          const selectedPid = pSelected && ids.includes(pSelected) ? pSelected : ids[0] || null;
          const selectedInfo = selectedPid ? infoObj[selectedPid] || {} : null;

          function setParam(pid, raw) {
            const info = infoObj[pid] || {};
            const v = raw === "" ? "" : Number(raw);

            setPVals((prev) => ({ ...prev, [pid]: v }));
            const err = validateParamValue(raw, info);
            setPErrs((prev) => ({ ...prev, [pid]: err }));
          }

          function pill(label, value, key, active, tone) {
            const cls = `wf-pill ${active ? "active" : ""} ${tone || ""}`.trim();
            return e(
              "button",
              {
                type: "button",
                className: cls,
                onClick: () => {
                  setPFilter(key);
                  setScrollTop(0);
                },
                title: `Filter: ${label}`,
              },
              `${label} ${value}`
            );
          }

          return e(
            "div",
            null,

            e(
              "div",
              {
                className: "wf-row",
                style: {
                  display: "grid",
                  gridTemplateColumns: "1fr auto auto",
                  gap: 8,
                  alignItems: "center",
                },
              },
              e("input", {
                className: "wf-input",
                value: pQuery,
                onChange: (ev) => {
                  setPQuery(ev.target.value);
                  setScrollTop(0);
                },
                placeholder: "Search parameters (id / name / desc)...",
              }),

              e(
                "label",
                {
                  style: {
                    display: "inline-flex",
                    alignItems: "center",
                    gap: 6,
                    marginBottom: 0,
                    fontSize: 13,
                  },
                },
                e("input", {
                  type: "checkbox",
                  checked: pRowDense,
                  onChange: (ev) => setPRowDense(ev.target.checked),
                }),
                "Dense"
              ),

              e(
                "button",
                {
                  type: "button",
                  className: "icon-btn",
                  onClick: resetAllParams,
                  title: "Reset all parameters to defaults",
                  disabled: !allIds.length,
                },
                "Reset all"
              )
            ),

            e(
              "div",
              {
                style: {
                  display: "flex",
                  gap: 8,
                  flexWrap: "wrap",
                  marginTop: 8,
                  marginBottom: 10,
                },
              },
              pill("Total", allIds.length, "all", pFilter === "all"),
              e("span", { className: "wf-pill is-muted" }, `Shown ${ids.length}`),
              pill("Changed", changedCount, "changed", pFilter === "changed", changedCount ? "is-info" : ""),
              pill("Invalid", invalidCount, "invalid", pFilter === "invalid", invalidCount ? "is-danger" : "")
            ),

            e(
              "div",
              {
                style: {
                  display: "grid",
                  gridTemplateColumns: "1.45fr 0.55fr",
                  gap: 10,
                },
              },

              e(
                "div",
                {
                  style: {
                    border: "1px solid rgba(0,0,0,0.08)",
                    borderRadius: 10,
                    overflow: "hidden",
                  },
                },

                e(
                  "div",
                  {
                    style: {
                      display: "grid",
                      gridTemplateColumns: "200px 1fr 90px",
                      gap: 8,
                      padding: "8px 10px",
                      borderBottom: "1px solid rgba(0,0,0,0.06)",
                      background: "rgba(0,0,0,0.02)",
                      fontSize: 12,
                      fontWeight: 600,
                    },
                  },
                  e("div", null, "Parameter"),
                  e("div", null, "Value"),
                  e("div", { style: { textAlign: "right" } }, "Status")
                ),

                e(
                  "div",
                  {
                    ref: listRef,
                    onScroll: (ev) => setScrollTop(ev.currentTarget.scrollTop),
                    style: {
                      height: 420,
                      overflowY: "auto",
                      position: "relative",
                    },
                  },
                  total === 0
                    ? e(
                        "div",
                        { className: "muted", style: { padding: 12 } },
                        pFilter === "changed"
                          ? "No changed parameters."
                          : pFilter === "invalid"
                            ? "No invalid parameters."
                            : "No parameters match the search."
                      )
                    : e(
                        "div",
                        { style: { height: totalH, position: "relative" } },
                        e(
                          "div",
                          {
                            style: {
                              position: "absolute",
                              top: startIdx * rowH,
                              left: 0,
                              right: 0,
                            },
                          },
                          visibleIds.map((pid) => {
                            const info = infoObj[pid] || {};
                            const v = pVals[pid] ?? info.default ?? "";
                            const err = pErrs[pid] || "";
                            const changed = isParamChanged(pid, info);
                            const selected = selectedPid === pid;

                            return e(
                              "div",
                              {
                                key: pid,
                                onClick: () => setPSelected(pid),
                                style: {
                                  display: "grid",
                                  gridTemplateColumns: "200px 1fr 90px",
                                  gap: 8,
                                  alignItems: "center",
                                  padding: "0 10px",
                                  height: rowH,
                                  cursor: "pointer",
                                  background: selected ? "rgba(44,120,255,0.08)" : "transparent",
                                  borderBottom: "1px solid rgba(0,0,0,0.04)",
                                },
                              },
                              e("div", { style: { fontSize: 12, fontWeight: 600 } }, pid),

                              e("input", {
                                type: "number",
                                step: "any",
                                value: v,
                                onClick: (ev) => ev.stopPropagation(),
                                onChange: (ev) => setParam(pid, ev.target.value),
                                style: {
                                  width: "100%",
                                  height: pRowDense ? 26 : 32,
                                  fontFamily:
                                    "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
                                  fontVariantNumeric: "tabular-nums",
                                },
                              }),

                              e(
                                "div",
                                {
                                  style: {
                                    fontSize: 12,
                                    textAlign: "right",
                                    color: err
                                      ? "#b00020"
                                      : changed
                                        ? "#1f5fd1"
                                        : "rgba(0,0,0,0.35)",
                                  },
                                },
                                err ? "Invalid" : changed ? "Changed" : ""
                              )
                            );
                          })
                        )
                      )
                )
              ),

              e(
                "div",
                {
                  style: {
                    border: "1px solid rgba(0,0,0,0.08)",
                    borderRadius: 10,
                    padding: 12,
                    height: "fit-content",
                  },
                },
                !selectedPid
                  ? e("div", { className: "muted" }, "Select a parameter to view details.")
                  : e(
                      "div",
                      null,

                      e(
                        "div",
                        {
                          style: {
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "center",
                            gap: 8,
                          },
                        },
                        e(
                          "div",
                          null,
                          e("div", { style: { fontWeight: 700, fontSize: 14 } }, selectedPid),
                          e("div", { className: "muted", style: { fontSize: 12 } }, selectedInfo?.name || "-")
                        ),
                        e(
                          "button",
                          {
                            type: "button",
                            className: "icon-btn",
                            title: "Reset to default",
                            onClick: () => resetSingleParam(selectedPid, selectedInfo),
                          },
                          "↺"
                        )
                      ),

                      e(
                        "div",
                        { style: { marginTop: 10, fontSize: 13 } },
                        e("div", { className: "muted", style: { marginBottom: 6 } }, "Details"),
                        e("div", null, `Unit: ${selectedInfo?.unit || "-"}`),
                        e("div", null, `Default: ${selectedInfo?.default ?? "-"}`),
                        e(
                          "div",
                          null,
                          `Range: ${selectedInfo?.minimum ?? "-"} ~ ${selectedInfo?.maximum ?? "-"}`
                        )
                      ),

                      selectedInfo?.desc
                        ? e(
                            "div",
                            { style: { marginTop: 10 } },
                            e("div", { className: "muted", style: { marginBottom: 6 } }, "Description"),
                            e("div", { style: { fontSize: 13, lineHeight: 1.35 } }, selectedInfo.desc)
                          )
                        : null,

                      pErrs[selectedPid]
                        ? e(
                            "div",
                            {
                              style: {
                                marginTop: 10,
                                color: "#b00020",
                                fontSize: 13,
                              },
                            },
                            pErrs[selectedPid]
                          )
                        : null
                    )
              )
            )
          );
        })()
      ),

      // ----------------------------------------------------------
      // Treatments
      // ----------------------------------------------------------
      e(
        "div",
        { className: "card wf-card" },
        renderSectionTitle("Treatments", `Selected: ${treatments.length}`),

        e(
          "div",
          { className: "wf-row" },
          e(
            "label",
            null,
            "Treatments",
            " ",
            e(
              "button",
              {
                type: "button",
                className: "icon-btn",
                title: "Select all treatments",
                onClick: () => setTreatments(availableTreatments.slice()),
              },
              "All"
            ),
            " ",
            e(
              "button",
              {
                type: "button",
                className: "icon-btn",
                title: "Clear treatments",
                onClick: () => setTreatments([]),
              },
              "None"
            )
          ),
          e(MultiSelect, {
            options: availableTreatments,
            selected: treatments,
            onChange: setTreatments,
            max: 30,
          })
        )
      ),

      // ----------------------------------------------------------
      // Notes
      // ----------------------------------------------------------
      e(
        "div",
        { className: "card wf-card" },
        renderSectionTitle("Notes", "Optional notes for this job."),
        e("textarea", {
          className: "wf-notes",
          rows: 5,
          value: notes,
          onChange: (ev) => setNotes(ev.target.value),
          placeholder: "Optional notes...",
        })
      )
    ),

    // ============================================================
    // Right panel
    // ============================================================
    e(
      "div",
      { className: "panel wf-right" },
      e(
        "div",
        { className: "wf-right-sticky" },
        e("h2", null, "Selected"),
        e("div", { className: "muted" }, "Review selections and submit from here."),

        e(
          "div",
          { className: "card wf-selected" },
          selectedSummary.map((r, idx) =>
            e(
              "div",
              { key: idx, className: "wf-sel-row" },
              e("div", { className: "wf-sel-k muted" }, r.k),
              e(
                "div",
                {
                  className: "wf-sel-v",
                  style: {
                    color:
                      r.k === "Parameters invalid" && invalidParamCount > 0
                        ? "#b00020"
                        : r.k === "Auto Forecast permission" && !canAutoForecastForSite
                          ? "#b26a00"
                          : undefined,
                  },
                },
                r.v
              )
            )
          )
        ),

        e("div", { style: { height: 12 } }),

        e(
          "div",
          { className: "card wf-selected" },
          e("div", { className: "wf-card-title" }, "Quick checks"),
          e(
            "div",
            { className: "wf-sel-row" },
            e("div", { className: "wf-sel-k muted" }, "Site"),
            e("div", { className: "wf-sel-v" }, site ? "OK" : "Missing")
          ),
          e(
            "div",
            { className: "wf-sel-row" },
            e("div", { className: "wf-sel-k muted" }, "Model"),
            e("div", { className: "wf-sel-v" }, model ? "OK" : "Missing")
          ),
          e(
            "div",
            { className: "wf-sel-row" },
            e("div", { className: "wf-sel-k muted" }, "Treatments"),
            e("div", { className: "wf-sel-v" }, treatments.length > 0 ? "OK" : "Missing")
          ),
          e(
            "div",
            { className: "wf-sel-row" },
            e("div", { className: "wf-sel-k muted" }, "Site access"),
            e(
              "div",
              { className: "wf-sel-v", style: { color: site ? "#1a7f37" : "#b26a00" } },
              site ? "Allowed" : "No site selected"
            )
          ),
          e(
            "div",
            { className: "wf-sel-row" },
            e("div", { className: "wf-sel-k muted" }, "Auto Forecast"),
            e(
              "div",
              { className: "wf-sel-v", style: { color: canAutoForecastForSite ? "#1a7f37" : "#b26a00" } },
              canAutoForecastForSite ? "Allowed" : "Not allowed"
            )
          ),
          e(
            "div",
            { className: "wf-sel-row" },
            e("div", { className: "wf-sel-k muted" }, "Validation"),
            e(
              "div",
              { className: "wf-sel-v", style: { color: hasParamError ? "#b00020" : "#1a7f37" } },
              hasParamError ? "Has errors" : "Passed"
            )
          ),
          task === "auto_forecast"
            ? e(
                "div",
                { className: "wf-sel-row" },
                e("div", { className: "wf-sel-k muted" }, "Mode"),
                e("div", { className: "wf-sel-v" }, runMode === "schedule" ? "Schedule" : "Run once")
              )
            : null
        ),

        e("div", { style: { height: 12 } }),

        e(
          "div",
          { className: "card wf-submit-card" },
          e(
            "div",
            { className: "wf-card-title" },
            runMode === "schedule" && task === "auto_forecast" ? "Create schedule" : "Submit"
          ),
          e("div", { className: "wf-submit-name" }, finalJobName || "Unnamed job"),

          e(
            "div",
            { className: "wf-submit-preview" },
            e("div", { className: "muted", style: { marginBottom: 6 } }, "Notes"),
            notesPreview
              ? e("div", { className: "wf-notes-preview" }, notesPreview)
              : e("div", { className: "muted" }, "No notes")
          ),

          task === "auto_forecast" && runMode === "schedule"
            ? e(
                "div",
                { className: "wf-submit-preview", style: { marginTop: 10 } },
                e("div", { className: "muted", style: { marginBottom: 6 } }, "Schedule"),
                e(
                  "div",
                  { className: "wf-notes-preview" },
                  `Run once now: ${runOnceAfterCreate ? "Yes" : "No"} · Cron: ${scheduleCron} · With DA: default · Also without DA: ${
                    autoForecastWithoutDA ? "Yes" : "No"
                  }`
                )
              )
            : null,

          e(
            "div",
            { className: "wf-submit-hint muted" },
            hasParamError
              ? `${invalidParamCount} invalid parameter(s) must be fixed before submission.`
              : task === "auto_forecast" && !canAutoForecastForSite
                ? `Auto Forecast is not available for site ${site || "-"}.`
                : accessibleSites.length === 0
                  ? "No site is available."
                  : task === "auto_forecast" && runMode === "schedule"
                    ? (runOnceAfterCreate ? "Ready to create schedule and run now." : "Ready to create schedule.")
                    : "Ready to submit."
          ),

          e(
            "button",
            {
              className: "btn primary wf-submit-btn",
              onClick: submit,
              disabled:
                submitLoading ||
                metaLoading ||
                wfPermLoading ||
                paramLoading ||
                !site ||
                !model ||
                treatments.length === 0 ||
                accessibleSites.length === 0 ||
                hasParamError ||
                (task === "auto_forecast" && !canAutoForecastForSite),
            },
            submitLoading
              ? task === "auto_forecast" && runMode === "schedule"
                ? "Creating..."
                : "Submitting..."
              : task === "auto_forecast" && runMode === "schedule"
                ? (runOnceAfterCreate ? "Create schedule + run now" : "Create schedule")
                : "Submit job"
          )
        )
      )
    ),

    // ============================================================
    // Site info modal
    // ============================================================
    e(
      Modal,
      {
        open: siteOpen,
        title: site ? `Site: ${site}` : "Site",
        onClose: () => setSiteOpen(false),
        width: 980,
      },
      siteLoading
        ? e("div", { className: "muted" }, "Loading site information...")
        : siteInfo && siteInfo.error
          ? e("div", { className: "error" }, siteInfo.error)
          : e(
              "div",
              null,

              e(
                "div",
                { className: "card", style: { marginBottom: 12 } },
                e("h3", null, "Basic information"),
                e("pre", { className: "wf-pre" }, JSON.stringify(siteInfo, null, 2))
              ),

              e(
                "div",
                { className: "card", style: { marginBottom: 12 } },
                e(
                  "div",
                  { className: "section-head" },
                  e("h3", null, "Forcing data"),
                  e("div", { className: "muted" }, "Select forcing variables to display")
                ),

                e(
                  "div",
                  { className: "ctrl" },
                  e("label", null, "Forcing variables"),
                  e(MultiSelect, {
                    options: ["temperature", "co2"],
                    selected: forcingVars,
                    onChange: async (nv) => {
                      setForcingVars(nv);
                      if (site) await loadForcing(site, nv);
                    },
                    max: 2,
                  })
                ),

                forcingData
                  ? e(
                      "div",
                      null,
                      forcingVars.map((v) =>
                        e(PlotSVG, {
                          key: v,
                          title: v,
                          x: forcingData.time || [],
                          y: forcingData[v] || [],
                          points: false,
                        })
                      )
                    )
                  : e("div", { className: "muted" }, "Loading forcing...")
              )
            )
    )
  );
}