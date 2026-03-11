import { api } from "../core/api.js";

function fmtLocalDateTime(value) {
  if (!value || typeof value !== "string") return "—";

  const m = value.match(
    /^(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/
  );

  if (!m) return value;

  return `${m[1]} ${m[2]}`;
}

function parseConfigJson(text) {
  if (!text || typeof text !== "string") return {};
  try {
    const obj = JSON.parse(text);
    return obj && typeof obj === "object" ? obj : {};
  } catch {
    return {};
  }
}

function getTaskPayload(task) {
  if (task?.config_json && typeof task.config_json === "string") {
    return parseConfigJson(task.config_json)?.payload || {};
  }
  if (task?.config && typeof task.config === "object") {
    return task.config?.payload || {};
  }
  if (task?.payload && typeof task.payload === "object") {
    return task.payload;
  }
  return {};
}

function asYesNo(v) {
  return v ? "Yes" : "No";
}

function runStatusMeta(status) {
  const s = String(status || "unknown").toLowerCase();
  if (s === "queued") return { cls: "queued", label: "Queued" };
  if (s === "running") return { cls: "running", label: "Running" };
  if (s === "done") return { cls: "done", label: "Done" };
  if (s === "failed") return { cls: "failed", label: "Failed" };
  if (s === "cancelled") return { cls: "cancelled", label: "Cancelled" };
  if (s === "enabled") return { cls: "running", label: "Enabled" };
  if (s === "disabled") return { cls: "cancelled", label: "Disabled" };
  return { cls: "unknown", label: status || "Unknown" };
}

function StatusPill({ status }) {
  const e = React.createElement;
  const meta = runStatusMeta(status);
  return e("span", { className: `jpill ${meta.cls}` }, meta.label);
}

function TaskInfoLine({ label, value }) {
  const e = React.createElement;
  return e(
    "div",
    { className: "sched-info-line" },
    e("span", { className: "sched-info-k" }, label),
    e("span", { className: "sched-info-v" }, value ?? "—")
  );
}

export function AccountSchedulesPanel({ auth, onViewRun }) {
  const e = React.createElement;

  const [tasks, setTasks] = React.useState([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState("");
  const [notice, setNotice] = React.useState("");

  const [meta, setMeta] = React.useState(null);
  const [runtime, setRuntime] = React.useState(null);

  const [expandedTasks, setExpandedTasks] = React.useState({});
  const [showRunsByTask, setShowRunsByTask] = React.useState({});
  const [taskRuns, setTaskRuns] = React.useState({});
  const [taskRunsLoading, setTaskRunsLoading] = React.useState({});
  const [expandedRuns, setExpandedRuns] = React.useState({});

  const [createOpen, setCreateOpen] = React.useState(false);
  const [newSiteId, setNewSiteId] = React.useState("");
  const [newModelId, setNewModelId] = React.useState("");
  const [newCron, setNewCron] = React.useState("0 * * * *");
  const [newEnabled, setNewEnabled] = React.useState(true);
  const [newTreatments, setNewTreatments] = React.useState("");
  const [newNotes, setNewNotes] = React.useState("");

  async function loadMeta() {
    try {
      const m = await api.workflowMeta();
      setMeta(m || {});
      const sites = m?.sites || [];
      const models = m?.models || [];
      setNewSiteId((prev) => prev || sites[0] || "");
      setNewModelId((prev) => prev || models[0] || "");
    } catch {
      setMeta({});
    }
  }

  async function loadRuntime() {
    if (!auth?.token) return;
    try {
      const j = await api.schedulerStatus(auth.token);
      setRuntime(j || null);
    } catch {
      setRuntime(null);
    }
  }

  async function loadTasks() {
    if (!auth?.token || !auth?.user) return;

    setLoading(true);
    setError("");
    setNotice("");

    try {
      const j = await api.schedulerTasksMine(auth.token, auth.user.id);
      setTasks(j?.tasks || []);
    } catch (ex) {
      setError(ex.message || "Failed to load schedules.");
      setTasks([]);
    } finally {
      setLoading(false);
    }
  }

  async function loadRunsForTask(taskId, { force = false } = {}) {
    if (!auth?.token || !taskId) return;
    if (!force && taskRuns[taskId]) return;

    setTaskRunsLoading((prev) => ({ ...prev, [taskId]: true }));

    try {
      const j = await api.schedulerTaskRuns(auth.token, taskId, 50);
      setTaskRuns((prev) => ({ ...prev, [taskId]: j?.runs || [] }));
    } catch (ex) {
      setError(ex.message || "Failed to load schedule runs.");
      setTaskRuns((prev) => ({ ...prev, [taskId]: [] }));
    } finally {
      setTaskRunsLoading((prev) => ({ ...prev, [taskId]: false }));
    }
  }

  React.useEffect(() => {
    if (!auth?.user || !auth?.token) return;
    loadMeta();
    loadRuntime();
    loadTasks();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth?.user, auth?.token]);

  React.useEffect(() => {
    const hasActive = (tasks || []).some((t) => Number(t?.active_run_count || 0) > 0);
    if (!hasActive) return;

    const timer = window.setInterval(() => {
      loadRuntime();
      loadTasks();
    }, 5000);

    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tasks, auth?.token]);

  async function handleCreateTask() {
    if (!auth?.token || !auth?.user) return;

    setError("");
    setNotice("");

    try {
      const treatments = String(newTreatments || "")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean);

      const payload = {
        site_id: String(newSiteId || "").trim(),
        model_id: String(newModelId || "").trim(),
        cron_expr: String(newCron || "").trim(),
        enabled: newEnabled ? 1 : 0,
        payload: {
          treatments,
          notes: String(newNotes || "").trim(),
        },
        created_by_user_id: Number(auth.user.id),
        created_by_username: String(auth.user.username || ""),
      };

      await api.schedulerCreateTask(auth.token, payload);
      setNotice("Schedule created.");
      setCreateOpen(false);
      await Promise.all([loadRuntime(), loadTasks()]);
    } catch (ex) {
      setError(ex.message || "Failed to create schedule.");
    }
  }

  async function handleRunOnce(taskId) {
    setError("");
    setNotice("");

    try {
      await api.schedulerRunOnce(auth.token, taskId);
      setNotice(`Task ${taskId} executed.`);
      await Promise.all([
        loadRuntime(),
        loadTasks(),
        loadRunsForTask(taskId, { force: true }),
      ]);
    } catch (ex) {
      setError(ex.message || "Failed to run schedule.");
    }
  }

  async function handleEnable(taskId) {
    setError("");
    setNotice("");

    try {
      await api.schedulerEnableTask(auth.token, taskId);
      setNotice(`Task ${taskId} enabled.`);
      await Promise.all([loadRuntime(), loadTasks()]);
    } catch (ex) {
      setError(ex.message || "Failed to enable schedule.");
    }
  }

  async function handleDisable(taskId) {
    setError("");
    setNotice("");

    try {
      await api.schedulerDisableTask(auth.token, taskId);
      setNotice(`Task ${taskId} disabled.`);
      await Promise.all([loadRuntime(), loadTasks()]);
    } catch (ex) {
      setError(ex.message || "Failed to disable schedule.");
    }
  }

  async function handleDelete(taskId) {
    if (!window.confirm(`Delete schedule ${taskId}?`)) return;

    setError("");
    setNotice("");

    try {
      await api.schedulerDeleteTask(auth.token, taskId);
      setNotice(`Task ${taskId} deleted.`);

      setExpandedTasks((prev) => {
        const next = { ...prev };
        delete next[taskId];
        return next;
      });

      setShowRunsByTask((prev) => {
        const next = { ...prev };
        delete next[taskId];
        return next;
      });

      setTaskRuns((prev) => {
        const next = { ...prev };
        delete next[taskId];
        return next;
      });

      await Promise.all([loadRuntime(), loadTasks()]);
    } catch (ex) {
      setError(ex.message || "Failed to delete schedule.");
    }
  }

  function toggleTask(taskId) {
    setExpandedTasks((prev) => {
      const nextOpen = !prev[taskId];
      return { ...prev, [taskId]: nextOpen };
    });
  }

  function toggleRuns(taskId) {
    setShowRunsByTask((prev) => {
      const nextOpen = !prev[taskId];
      const next = { ...prev, [taskId]: nextOpen };
      if (nextOpen) loadRunsForTask(taskId);
      return next;
    });
  }

  function toggleRun(runId) {
    setExpandedRuns((prev) => ({ ...prev, [runId]: !prev[runId] }));
  }

  return e(
    "div",
    { style: { marginTop: 12 } },

    e(
      "div",
      { className: "card" },

      e(
        "div",
        { className: "section-head" },
        e("h3", null, "Schedules"),
        e(
          "div",
          null,
          e(
            "button",
            {
              type: "button",
              className: "btn",
              onClick: async () => {
                await Promise.all([loadRuntime(), loadTasks()]);
              },
              disabled: loading,
            },
            loading ? "Loading..." : "Refresh"
          ),
          e(
            "button",
            {
              type: "button",
              className: "btn primary",
              style: { marginLeft: 8 },
              onClick: () => setCreateOpen((v) => !v),
            },
            createOpen ? "Close create" : "New schedule"
          )
        )
      ),

      e(
        "div",
        { className: "muted", style: { marginBottom: 10 } },
        "This page shows only schedules created by the current user. Click task info to expand or collapse task details."
      ),

      runtime
        ? e(
            "div",
            { className: "sched-runtime-box" },
            e("div", { className: "sched-runtime-title" }, "Scheduler runtime"),
            e(
              "div",
              { className: "sched-runtime-line muted" },
              `Initialized: ${asYesNo(runtime.initialized)} · Running: ${asYesNo(runtime.running)} · Registered jobs: ${(runtime.jobs || []).length}`
            )
          )
        : null,

      error ? e("div", { className: "error", style: { marginBottom: 8 } }, error) : null,
      notice ? e("div", { className: "admin-notice", style: { marginBottom: 8 } }, notice) : null,

      createOpen
        ? e(
            "div",
            { className: "sched-create-box" },

            e("div", { className: "sched-create-title" }, "Create schedule"),

            e(
              "div",
              { className: "sched-create-grid" },

              e(
                "div",
                { className: "ctrl" },
                e("label", null, "Site"),
                e(
                  "select",
                  {
                    value: newSiteId,
                    onChange: (ev) => setNewSiteId(ev.target.value),
                  },
                  (meta?.sites || []).map((siteId) =>
                    e("option", { key: siteId, value: siteId }, siteId)
                  )
                )
              ),

              e(
                "div",
                { className: "ctrl" },
                e("label", null, "Model"),
                e(
                  "select",
                  {
                    value: newModelId,
                    onChange: (ev) => setNewModelId(ev.target.value),
                  },
                  (meta?.models || []).map((modelId) =>
                    e("option", { key: modelId, value: modelId }, modelId)
                  )
                )
              ),

              e(
                "div",
                { className: "ctrl" },
                e("label", null, "Cron expression"),
                e("input", {
                  type: "text",
                  value: newCron,
                  onChange: (ev) => setNewCron(ev.target.value),
                  placeholder: "e.g. */30 * * * *",
                })
              ),

              e(
                "div",
                { className: "ctrl" },
                e("label", null, "Enabled"),
                e(
                  "label",
                  { className: "admin-check-row" },
                  e("input", {
                    type: "checkbox",
                    checked: !!newEnabled,
                    onChange: (ev) => setNewEnabled(ev.target.checked),
                  }),
                  e("span", null, newEnabled ? "Enabled" : "Disabled")
                )
              ),

              e(
                "div",
                { className: "ctrl", style: { gridColumn: "1 / -1" } },
                e("label", null, "Treatments (comma-separated)"),
                e("input", {
                  type: "text",
                  value: newTreatments,
                  onChange: (ev) => setNewTreatments(ev.target.value),
                  placeholder: "treatment_1,treatment_2",
                })
              ),

              e(
                "div",
                { className: "ctrl", style: { gridColumn: "1 / -1" } },
                e("label", null, "Notes"),
                e("textarea", {
                  rows: 3,
                  value: newNotes,
                  onChange: (ev) => setNewNotes(ev.target.value),
                })
              )
            ),

            e(
              "div",
              { className: "admin-actions" },
              e(
                "button",
                {
                  type: "button",
                  className: "btn primary",
                  onClick: handleCreateTask,
                },
                "Create schedule"
              )
            )
          )
        : null,

      loading && tasks.length === 0
        ? e("div", { className: "muted" }, "Loading schedules...")
        : tasks.length === 0
          ? e("div", { className: "muted" }, "No schedules.")
          : e(
              "div",
              { className: "sched-list" },
              ...tasks.map((task) => {
                const taskId = task.id;
                const taskOpen = !!expandedTasks[taskId];
                const runsOpen = !!showRunsByTask[taskId];
                const runs = taskRuns[taskId] || [];
                const runsLoading = !!taskRunsLoading[taskId];
                const payload = getTaskPayload(task);
                const treatments = Array.isArray(payload?.treatments) ? payload.treatments : [];

                return e(
                  "div",
                  { key: taskId, className: "sched-card" },

                  e(
                    "div",
                    {
                      className: "sched-card-head",
                      role: "button",
                      tabIndex: 0,
                      onClick: () => toggleTask(taskId),
                      onKeyDown: (ev) => {
                        if (ev.key === "Enter" || ev.key === " ") {
                          ev.preventDefault();
                          toggleTask(taskId);
                        }
                      },
                      style: {
                        cursor: "pointer",
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "flex-start",
                        gap: 12,
                      },
                    },

                    e(
                      "div",
                      {
                        className: "sched-head-main",
                        style: { minWidth: 0, flex: "1 1 auto" },
                      },
                      e(
                        "div",
                        { className: "sched-title-row" },
                        e(
                          "div",
                          {
                            className: "sched-task-id",
                            style: {
                              whiteSpace: "normal",
                              wordBreak: "break-word",
                              lineHeight: 1.3,
                            },
                          },
                          `Task ${taskId}`
                        ),
                        e(StatusPill, {
                          status: task.task_state || task.last_run_status || "unknown",
                        })
                      ),
                      e(
                        "div",
                        {
                          className: "sched-subtitle muted",
                          style: {
                            marginTop: 4,
                            whiteSpace: "normal",
                            wordBreak: "break-word",
                          },
                        },
                        `${task.site_id || "—"} · ${task.model_id || "—"} · ${task.task_type || "auto_forecast"}`
                      ),
                      e(
                        "div",
                        { className: "muted", style: { marginTop: 6 } },
                        `Last run: ${fmtLocalDateTime(task.last_run_at)}`
                      )
                    )
                  ),

                  !taskOpen
                    ? null
                    : e(
                        React.Fragment,
                        null,
                        e(
                          "div",
                          { className: "sched-summary-grid" },
                          e(TaskInfoLine, { label: "Owner", value: task.created_by_username || "—" }),
                          e(TaskInfoLine, { label: "Enabled", value: asYesNo(task.enabled) }),
                          e(TaskInfoLine, { label: "Cron", value: task.cron_expr || "—" }),
                          e(TaskInfoLine, { label: "Run count", value: task.run_count ?? "0" }),
                          e(TaskInfoLine, { label: "Active runs", value: String(task.active_run_count ?? 0) }),
                          e(TaskInfoLine, { label: "Last run", value: fmtLocalDateTime(task.last_run_at) }),
                          e(TaskInfoLine, { label: "Next run", value: fmtLocalDateTime(task.next_run_at) }),
                          e(TaskInfoLine, { label: "Latest run ID", value: task.last_run_id || "—" })
                        ),

                        treatments.length > 0
                          ? e(
                              "div",
                              { className: "sched-chip-row" },
                              ...treatments.map((t) =>
                                e("span", { key: t, className: "jchip t" }, t)
                              )
                            )
                          : null,

                        payload?.notes
                          ? e(
                              "div",
                              { className: "muted", style: { marginTop: 8 } },
                              `Notes: ${payload.notes}`
                            )
                          : null,

                        e(
                          "div",
                          {
                            className: "sched-head-actions",
                            style: { marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" },
                          },
                          e(
                            "button",
                            {
                              type: "button",
                              className: "btn",
                              onClick: (ev) => {
                                ev.preventDefault();
                                ev.stopPropagation();
                                toggleRuns(taskId);
                              },
                            },
                            runsOpen ? "Hide runs" : "View runs"
                          ),
                          e(
                            "button",
                            {
                              type: "button",
                              className: "btn",
                              onClick: (ev) => {
                                ev.preventDefault();
                                ev.stopPropagation();
                                handleRunOnce(taskId);
                              },
                            },
                            "Run once"
                          ),
                          task.enabled
                            ? e(
                                "button",
                                {
                                  type: "button",
                                  className: "btn",
                                  onClick: (ev) => {
                                    ev.preventDefault();
                                    ev.stopPropagation();
                                    handleDisable(taskId);
                                  },
                                },
                                "Disable"
                              )
                            : e(
                                "button",
                                {
                                  type: "button",
                                  className: "btn",
                                  onClick: (ev) => {
                                    ev.preventDefault();
                                    ev.stopPropagation();
                                    handleEnable(taskId);
                                  },
                                },
                                "Enable"
                              ),
                          e(
                            "button",
                            {
                              type: "button",
                              className: "btn",
                              onClick: (ev) => {
                                ev.preventDefault();
                                ev.stopPropagation();
                                handleDelete(taskId);
                              },
                            },
                            "Delete"
                          )
                        ),

                        !runsOpen
                          ? null
                          : e(
                              "div",
                              { className: "sched-runs-wrap" },

                              e(
                                "div",
                                { className: "section-head", style: { marginTop: 10 } },
                                e("h4", null, "Runs"),
                                e(
                                  "div",
                                  { className: "muted" },
                                  runsLoading
                                    ? "Loading..."
                                    : runs.length === 0
                                      ? "No runs"
                                      : `${runs.length} run(s)`
                                )
                              ),

                              runsLoading
                                ? e("div", { className: "muted" }, "Loading runs...")
                                : runs.length === 0
                                  ? e("div", { className: "muted" }, "No runs for this task yet.")
                                  : e(
                                      "div",
                                      { className: "sched-run-list" },
                                      ...runs.map((run) => {
                                        const runId = run.id;
                                        const runOpen = !!expandedRuns[runId];

                                        return e(
                                          "div",
                                          { key: runId, className: "sched-run-card" },

                                          e(
                                            "div",
                                            { className: "sched-run-head" },

                                            e(
                                              "div",
                                              { className: "sched-run-left" },
                                              e(
                                                "div",
                                                { className: "sched-run-title-row" },
                                                e("div", { className: "sched-run-id" }, runId),
                                                e(StatusPill, { status: run.status })
                                              ),
                                              e(
                                                "div",
                                                { className: "muted" },
                                                `${run.trigger_type || "—"} · ${fmtLocalDateTime(
                                                  run.finished_at || run.updated_at || run.created_at
                                                )}`
                                              )
                                            ),

                                            e(
                                              "div",
                                              { className: "sched-run-actions" },
                                              e(
                                                "button",
                                                {
                                                  type: "button",
                                                  className: "btn",
                                                  onClick: () => toggleRun(runId),
                                                },
                                                runOpen ? "Hide details" : "Show details"
                                              ),
                                              e(
                                                "button",
                                                {
                                                  type: "button",
                                                  className: "btn primary",
                                                  onClick: () => onViewRun && onViewRun(run),
                                                },
                                                "View"
                                              )
                                            )
                                          ),

                                          !runOpen
                                            ? null
                                            : e(
                                                "div",
                                                { className: "sched-run-detail-grid" },
                                                e(TaskInfoLine, { label: "Run ID", value: run.id || "—" }),
                                                e(TaskInfoLine, { label: "Schedule ID", value: run.scheduled_task_id ?? "—" }),
                                                e(TaskInfoLine, { label: "Status", value: run.status || "—" }),
                                                e(TaskInfoLine, { label: "Trigger", value: run.trigger_type || "—" }),
                                                e(TaskInfoLine, { label: "Site", value: run.site_id || "—" }),
                                                e(TaskInfoLine, { label: "Model", value: run.model_id || "—" }),
                                                e(TaskInfoLine, { label: "Task type", value: run.task_type || "—" }),
                                                e(TaskInfoLine, { label: "Created", value: fmtLocalDateTime(run.created_at) }),
                                                e(TaskInfoLine, { label: "Started", value: fmtLocalDateTime(run.started_at) }),
                                                e(TaskInfoLine, { label: "Finished", value: fmtLocalDateTime(run.finished_at) }),
                                                e(TaskInfoLine, { label: "Updated", value: fmtLocalDateTime(run.updated_at) }),
                                                e(TaskInfoLine, { label: "User", value: run.username || "—" }),
                                                run.error_message
                                                  ? e(
                                                      "div",
                                                      {
                                                        className: "sched-run-error",
                                                        style: { gridColumn: "1 / -1" },
                                                      },
                                                      e("div", { className: "sched-info-k" }, "Error"),
                                                      e("div", null, String(run.error_message))
                                                    )
                                                  : null
                                              )
                                        );
                                      })
                                    )
                            )
                      )
                );
              })
            )
    )
  );
}