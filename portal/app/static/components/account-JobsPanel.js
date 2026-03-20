import { api } from "../core/api.js";
import { AccountJobsDownloadModal } from "./account-JobsDownload.js";

function fmtIsoCompact(value) {
  if (!value || typeof value !== "string") return "—";

  const m = value.match(
    /^(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/
  );
  if (!m) return value;

  const date = m[1];
  const time = m[2];
  return `${date} ${time}`;
}

function statusMeta(status) {
  const s = String(status || "unknown").toLowerCase();
  if (s === "queued") return { s, label: "Queued" };
  if (s === "running") return { s, label: "Running" };
  if (s === "done") return { s, label: "Done" };
  if (s === "failed") return { s, label: "Failed" };
  if (s === "cancelled") return { s, label: "Cancelled" };
  return { s: "unknown", label: status || "Unknown" };
}

function StatusPill({ status }) {
  const e = React.createElement;
  const meta = statusMeta(status);
  return e("span", { className: `jpill ${meta.s}` }, meta.label);
}

function ChipsLine({ label, items = [], max = 4, variant = "" }) {
  const e = React.createElement;
  const [open, setOpen] = React.useState(false);

  const list = Array.isArray(items) ? items : [];
  const showAll = open || list.length <= max;
  const shown = showAll ? list : list.slice(0, max);
  const rest = Math.max(0, list.length - shown.length);

  return e(
    "div",
    { className: "jrow" },
    e("div", { className: "jk muted" }, label),
    e(
      "div",
      { className: "jv" },
      list.length === 0
        ? e("span", { className: "muted" }, "—")
        : e(
            "div",
            { className: "jchips" },
            ...shown.map((x) =>
              e("span", { key: x, className: `jchip ${variant}` }, x)
            ),
            rest > 0 && !open
              ? e(
                  "button",
                  {
                    type: "button",
                    className: "jchip more",
                    onClick: (ev) => {
                      ev.preventDefault();
                      ev.stopPropagation();
                      setOpen(true);
                    },
                  },
                  `+${rest}`
                )
              : null,
            list.length > max && open
              ? e(
                  "button",
                  {
                    type: "button",
                    className: "jchip more",
                    onClick: (ev) => {
                      ev.preventDefault();
                      ev.stopPropagation();
                      setOpen(false);
                    },
                  },
                  "Collapse"
                )
              : null
          )
    )
  );
}

function canDeleteJob(job) {
  const s = String(job?.status || "").toLowerCase();
  return s === "done" || s === "failed" || s === "cancelled";
}

function displayRunTime(job) {
  return job?.finished_at || job?.updated_at || job?.created_at || "";
}

function displayTrigger(job) {
  const t = String(job?.trigger_type || "").trim();
  if (!t) return "—";
  if (t === "scheduled") return "Scheduled";
  if (t === "manual") return "Manual";
  if (t === "system") return "System";
  return t;
}

export function AccountJobsPanel({
  auth,
  jobs,
  loadingRun,
  onView,
  onRefreshJobs,
}) {
  const e = React.createElement;
  const [busyId, setBusyId] = React.useState("");
  const [expanded, setExpanded] = React.useState({});
  const [downloadJob, setDownloadJob] = React.useState(null);

  function toggleJob(jobId) {
    setExpanded((prev) => ({ ...prev, [jobId]: !prev[jobId] }));
  }

  function handleDownload(job, ev) {
    ev.preventDefault();
    ev.stopPropagation();
    setDownloadJob(job);
  }

  async function handleDelete(job, ev) {
    ev.preventDefault();
    ev.stopPropagation();

    if (!canDeleteJob(job)) return;

    const ok = window.confirm(
      `Delete run ${job.id} from the database and remove its site files?\n\nThis action cannot be undone.`
    );
    if (!ok) return;

    setBusyId(`delete-${job.id}`);
    try {
      await api.accountDeleteJob(auth.token, job.id);

      if (downloadJob?.id === job.id) {
        setDownloadJob(null);
      }

      await onRefreshJobs?.();
    } catch (ex) {
      alert(ex.message || "Failed to delete run.");
    } finally {
      setBusyId("");
    }
  }

  if (!jobs || jobs.length === 0) {
    return e(
      React.Fragment,
      null,
      e(
        "div",
        { className: "muted", style: { marginTop: 10 } },
        "No jobs yet. Submit one in Custom Workflow."
      )
    );
  }

  return e(
    React.Fragment,
    null,

    e(AccountJobsDownloadModal, {
      auth,
      open: !!downloadJob,
      job: downloadJob,
      onClose: () => setDownloadJob(null),
    }),

    e(
      "div",
      {
        className: "job-grid2",
        style: {
          display: "grid",
          gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
          gap: 14,
          alignItems: "start",
        },
      },
      jobs.map((job) => {
        const isOpen = !!expanded[job.id];

        return e(
          "div",
          {
            key: job.id,
            className: "jobcard3",
          },

          e(
            "div",
            {
              className: "jhead",
              role: "button",
              tabIndex: 0,
              onClick: () => toggleJob(job.id),
              onKeyDown: (ev) => {
                if (ev.key === "Enter" || ev.key === " ") {
                  ev.preventDefault();
                  toggleJob(job.id);
                }
              },
              style: {
                cursor: "pointer",
                display: "flex",
                alignItems: "flex-start",
                justifyContent: "space-between",
                gap: 12,
              },
              title: isOpen ? "Collapse job details" : "Expand job details",
            },
            e(
              "div",
              {
                className: "jtitlewrap",
                style: {
                  minWidth: 0,
                  flex: "1 1 auto",
                },
              },
              e(
                "div",
                {
                  className: "jtitle",
                  style: {
                    whiteSpace: "normal",
                    wordBreak: "break-word",
                    lineHeight: 1.3,
                  },
                },
                job.name || "(unnamed job)"
              ),
              e(
                "div",
                {
                  className: "jsub muted",
                  style: {
                    marginTop: 4,
                    whiteSpace: "normal",
                    wordBreak: "break-word",
                  },
                },
                e("span", { className: "jmono" }, `#${job.id}`),
                e("span", { className: "jdot" }, "•"),
                e("span", null, `${job.site || "—"} · ${job.task || "—"}`)
              )
            ),
            e(
              "div",
              {
                style: {
                  flex: "0 0 auto",
                  display: "flex",
                  alignItems: "center",
                },
              },
              e(StatusPill, { status: job.status })
            )
          ),

          e(
            "div",
            {
              className: "jmeta",
              role: "button",
              tabIndex: 0,
              onClick: () => toggleJob(job.id),
              onKeyDown: (ev) => {
                if (ev.key === "Enter" || ev.key === " ") {
                  ev.preventDefault();
                  toggleJob(job.id);
                }
              },
              style: { cursor: "pointer" },
            },
            e("span", { className: "muted" }, "Run time: "),
            e("span", null, fmtIsoCompact(displayRunTime(job)))
          ),

          !isOpen
            ? null
            : e(
                React.Fragment,
                null,
                e(
                  "div",
                  { className: "jbody" },
                  e(
                    "div",
                    { className: "jrow" },
                    e("div", { className: "jk muted" }, "Trigger"),
                    e("div", { className: "jv" }, displayTrigger(job))
                  ),
                  e(
                    "div",
                    { className: "jrow" },
                    e("div", { className: "jk muted" }, "Schedule"),
                    e("div", { className: "jv" }, job.scheduled_task_id ?? "—")
                  ),
                  e(ChipsLine, {
                    label: "Models",
                    items: job.models || [],
                    max: 3,
                    variant: "m",
                  }),
                  e(ChipsLine, {
                    label: "Treatments",
                    items: job.treatments || [],
                    max: 4,
                    variant: "t",
                  }),
                  job.error_message
                    ? e(
                        "div",
                        { className: "jrow" },
                        e("div", { className: "jk muted" }, "Error"),
                        e("div", { className: "jv" }, String(job.error_message))
                      )
                    : null
                ),

                e(
                  "div",
                  {
                    className: "jfoot",
                    style: { display: "flex", gap: 8, flexWrap: "wrap" },
                  },
                  e(
                    "button",
                    {
                      type: "button",
                      className: "btn primary",
                      onClick: (ev) => {
                        ev.preventDefault();
                        ev.stopPropagation();
                        onView(job);
                      },
                      disabled: loadingRun,
                    },
                    loadingRun ? "Loading..." : "View"
                  ),

                  e(
                    "button",
                    {
                      type: "button",
                      className: "btn",
                      onClick: (ev) => handleDownload(job, ev),
                    },
                    "Download"
                  ),

                  canDeleteJob(job)
                    ? e(
                        "button",
                        {
                          type: "button",
                          className: "btn",
                          onClick: (ev) => handleDelete(job, ev),
                          disabled: busyId === `delete-${job.id}`,
                        },
                        busyId === `delete-${job.id}` ? "Deleting..." : "Delete"
                      )
                    : null
                )
              )
        );
      })
    )
  );
}