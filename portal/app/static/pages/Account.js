import { AppCtx } from "../core/context.js";
import { api } from "../core/api.js";
import { AccountJobsPanel } from "../components/account-JobsPanel.js";
import { AccountJobViewerModal } from "../components/account-JobViewerModal.js";
import { AccountAdminPanel } from "../components/account-AdminPanel.js";
import { AccountSchedulesPanel } from "../components/account-SchedulesPanel.js";

function mergeJobsKeepOld(prevJobs, nextJobs) {
  const byId = new Map((prevJobs || []).map((j) => [String(j.id), j]));

  return (nextJobs || []).map((nextJob) => {
    const prevJob = byId.get(String(nextJob.id)) || {};

    return {
      ...prevJob,
      ...nextJob,

      name: (nextJob.name && String(nextJob.name).trim()) ? nextJob.name : prevJob.name,
      site: (nextJob.site && String(nextJob.site).trim()) ? nextJob.site : prevJob.site,
      task: (nextJob.task && String(nextJob.task).trim()) ? nextJob.task : prevJob.task,

      created_at: (nextJob.created_at && String(nextJob.created_at).trim())
        ? nextJob.created_at
        : prevJob.created_at,

      updated_at: (nextJob.updated_at && String(nextJob.updated_at).trim())
        ? nextJob.updated_at
        : prevJob.updated_at,

      finished_at: (nextJob.finished_at && String(nextJob.finished_at).trim())
        ? nextJob.finished_at
        : prevJob.finished_at,

      started_at: (nextJob.started_at && String(nextJob.started_at).trim())
        ? nextJob.started_at
        : prevJob.started_at,

      models: (Array.isArray(nextJob.models) && nextJob.models.length)
        ? nextJob.models
        : (prevJob.models || []),

      treatments: (Array.isArray(nextJob.treatments) && nextJob.treatments.length)
        ? nextJob.treatments
        : (prevJob.treatments || []),

      scheduled_task_id:
        nextJob.scheduled_task_id !== undefined
          ? nextJob.scheduled_task_id
          : prevJob.scheduled_task_id,

      trigger_type:
        nextJob.trigger_type !== undefined
          ? nextJob.trigger_type
          : prevJob.trigger_type,

      error_message:
        nextJob.error_message !== undefined
          ? nextJob.error_message
          : prevJob.error_message,
    };
  });
}

function normalizeSitePermissions(payload) {
  const raw = payload?.site_permissions || {};
  const out = {};

  Object.keys(raw).forEach((siteId) => {
    const item = raw[siteId] || {};
    out[siteId] = {
      can_auto_forecast: !!item.can_auto_forecast,
    };
  });

  return out;
}

function TabChip({ active, onClick, children }) {
  const e = React.createElement;
  return e(
    "button",
    {
      type: "button",
      className: `chip ${active ? "active" : ""}`,
      onClick,
    },
    children
  );
}

export function Account() {
  const e = React.createElement;
  const { auth, navigate, logout } = React.useContext(AppCtx);

  const [tab, setTab] = React.useState("jobs");
  const [err, setErr] = React.useState("");

  const [jobs, setJobs] = React.useState([]);
  const [loadingJobs, setLoadingJobs] = React.useState(false);
  const [loadingRun, setLoadingRun] = React.useState(false);

  const [open, setOpen] = React.useState(false);
  const [activeJob, setActiveJob] = React.useState(null);
  const [variable, setVariable] = React.useState("GPP");
  const [result, setResult] = React.useState(null);

  const [manifest, setManifest] = React.useState(null);
  const [selModel, setSelModel] = React.useState("");
  const [selTreatment, setSelTreatment] = React.useState("");

  const isSuperuser = auth.user?.role === "superuser";

  // Admin state
  const [schedulerStatus, setSchedulerStatus] = React.useState(null);
  const [schedulerLoading, setSchedulerLoading] = React.useState(false);
  const [schedulerError, setSchedulerError] = React.useState("");
  const [schedulerNotice, setSchedulerNotice] = React.useState("");

  const [cleanupCandidates, setCleanupCandidates] = React.useState([]);
  const [cleanupLogs, setCleanupLogs] = React.useState([]);
  const [cleanupLoading, setCleanupLoading] = React.useState(false);
  const [cleanupError, setCleanupError] = React.useState("");
  const [cleanupNotice, setCleanupNotice] = React.useState("");
  const [ttlEphemeral, setTtlEphemeral] = React.useState(7);
  const [ttlNormal, setTtlNormal] = React.useState(90);

  const [allUsers, setAllUsers] = React.useState([]);
  const [usersLoading, setUsersLoading] = React.useState(false);
  const [usersError, setUsersError] = React.useState("");

  const [adminSites, setAdminSites] = React.useState([]);
  const [sitesLoading, setSitesLoading] = React.useState(false);
  const [sitesError, setSitesError] = React.useState("");

  const [selectedUserId, setSelectedUserId] = React.useState("");
  const [selectedUser, setSelectedUser] = React.useState(null);

  const [sitePermissions, setSitePermissions] = React.useState({});
  const [permLoading, setPermLoading] = React.useState(false);
  const [permSaving, setPermSaving] = React.useState(false);
  const [permError, setPermError] = React.useState("");
  const [permNotice, setPermNotice] = React.useState("");

  function deriveModels(man) {
    if (man?.request?.models && man.request.models.length) return man.request.models;
    if (man?.request?.model) return [man.request.model];
    return [];
  }

  function deriveTreatments(man) {
    if (man?.request?.treatments && man.request.treatments.length) return man.request.treatments;
    if (man?.request?.treatment) return [man.request.treatment];
    return [];
  }

  function deriveVariables(man, model, treatment) {
    if (man?.outputs?.index && model && treatment) {
      return Object.keys(man.outputs.index?.[model]?.[treatment] || {});
    }
    if (man?.outputs?.timeseries) {
      return Object.keys(man.outputs.timeseries || {});
    }
    return [];
  }

  async function loadJobs({ merge = true } = {}) {
    if (!auth.user) return;

    setLoadingJobs(true);
    setErr("");

    try {
      const j = await api.accountJobs(auth.token);
      const next = j.jobs || [];
      setJobs((prev) => (merge ? mergeJobsKeepOld(prev, next) : next));
    } catch (ex) {
      setErr(ex.message || String(ex));
    } finally {
      setLoadingJobs(false);
    }
  }

  async function loadScheduler() {
    if (!isSuperuser) return;

    setSchedulerLoading(true);
    setSchedulerError("");
    setSchedulerNotice("");

    try {
      const status = await api.schedulerStatus(auth.token);
      setSchedulerStatus(status || { initialized: false, running: false, jobs: [] });
    } catch (ex) {
      setSchedulerError(ex.message || "Failed to load scheduler runtime.");
      setSchedulerStatus({ initialized: false, running: false, jobs: [] });
    } finally {
      setSchedulerLoading(false);
    }
  }

  async function loadCleanup() {
    if (!isSuperuser) return;

    setCleanupLoading(true);
    setCleanupError("");
    setCleanupNotice("");

    try {
      const [cand, logs] = await Promise.all([
        api.cleanupCandidates
          ? api.cleanupCandidates(auth.token, {
              ttl_days_ephemeral: ttlEphemeral,
              ttl_days_normal: ttlNormal,
            })
          : Promise.resolve({ candidates: [] }),
        api.cleanupLogs
          ? api.cleanupLogs(auth.token)
          : Promise.resolve({ logs: [] }),
      ]);

      setCleanupCandidates(cand?.candidates || []);
      setCleanupLogs(logs?.logs || []);
    } catch (ex) {
      setCleanupError(ex.message || "Failed to load cleanup info.");
      setCleanupCandidates([]);
      setCleanupLogs([]);
    } finally {
      setCleanupLoading(false);
    }
  }

  async function loadUsers() {
    if (!isSuperuser || !auth.token) return;

    setUsersLoading(true);
    setUsersError("");

    try {
      const j = await api.adminUsers(auth.token);
      const users = j?.users || [];

      setAllUsers(users);

      const nonSelf = users.filter((u) => String(u.id) !== String(auth.user?.id));
      const firstUser = nonSelf[0] || null;

      if (firstUser) {
        setSelectedUserId(String(firstUser.id));
        setSelectedUser(firstUser);
      } else {
        setSelectedUserId("");
        setSelectedUser(null);
      }
    } catch (ex) {
      setUsersError(ex.message || "Failed to load users.");
      setAllUsers([]);
      setSelectedUserId("");
      setSelectedUser(null);
    } finally {
      setUsersLoading(false);
    }
  }

  async function loadAdminSites() {
    if (!isSuperuser || !auth.token) return;

    setSitesLoading(true);
    setSitesError("");

    try {
      const j = await api.adminSites(auth.token);
      setAdminSites(j?.sites || []);
    } catch (ex) {
      setSitesError(ex.message || "Failed to load sites.");
      setAdminSites([]);
    } finally {
      setSitesLoading(false);
    }
  }

  async function loadUserPermissions(userId) {
    if (!isSuperuser || !auth.token || !userId) {
      setSitePermissions({});
      return;
    }

    setPermLoading(true);
    setPermError("");
    setPermNotice("");

    try {
      const j = await api.adminUserPermissions(auth.token, userId);
      setSitePermissions(normalizeSitePermissions(j));
    } catch (ex) {
      setPermError(ex.message || "Failed to load permissions.");
      setSitePermissions({});
    } finally {
      setPermLoading(false);
    }
  }

  function updateSitePermission(siteId, checked) {
    setPermNotice("");

    setSitePermissions((prev) => {
      const next = { ...(prev || {}) };
      next[siteId] = {
        can_auto_forecast: !!checked,
      };
      return next;
    });
  }

  async function savePermissions() {
    if (!isSuperuser || !auth.token || !selectedUserId) return;

    setPermSaving(true);
    setPermError("");
    setPermNotice("");

    try {
      for (const siteId of adminSites) {
        const row = sitePermissions?.[siteId] || {
          can_auto_forecast: false,
        };

        await api.adminSavePermission(auth.token, selectedUserId, {
          site_id: siteId,
          can_auto_forecast: !!row.can_auto_forecast,
        });
      }

      setPermNotice("Permissions saved.");
      await loadUserPermissions(selectedUserId);
    } catch (ex) {
      setPermError(ex.message || "Failed to save permissions.");
    } finally {
      setPermSaving(false);
    }
  }

  async function reloadSchedulerJobs() {
    if (!api.schedulerReload) return;

    setSchedulerLoading(true);
    setSchedulerError("");
    setSchedulerNotice("");

    try {
      await api.schedulerReload(auth.token);
      setSchedulerNotice("Scheduler runtime reloaded.");
      await loadScheduler();
    } catch (ex) {
      setSchedulerError(ex.message || "Failed to reload scheduler runtime.");
    } finally {
      setSchedulerLoading(false);
    }
  }

  async function cleanupDryRun() {
    if (!api.cleanupDryRun) return;

    setCleanupLoading(true);
    setCleanupError("");
    setCleanupNotice("");

    try {
      const res = await api.cleanupDryRun(auth.token, {
        ttl_days_ephemeral: Number(ttlEphemeral) || 7,
        ttl_days_normal: Number(ttlNormal) || 90,
      });

      setCleanupCandidates((res && res.results) || []);
      setCleanupNotice("Cleanup dry-run completed.");
    } catch (ex) {
      setCleanupError(ex.message || "Cleanup dry-run failed.");
    } finally {
      setCleanupLoading(false);
    }
  }

  async function cleanupRun() {
    if (!api.cleanupRun) return;
    if (!window.confirm("Run cleanup now?")) return;

    setCleanupLoading(true);
    setCleanupError("");
    setCleanupNotice("");

    try {
      await api.cleanupRun(auth.token, {
        ttl_days_ephemeral: Number(ttlEphemeral) || 7,
        ttl_days_normal: Number(ttlNormal) || 90,
      });

      setCleanupNotice("Cleanup executed.");
      await loadCleanup();
      await loadJobs({ merge: false });
    } catch (ex) {
      setCleanupError(ex.message || "Cleanup failed.");
    } finally {
      setCleanupLoading(false);
    }
  }

  React.useEffect(() => {
    if (auth.status !== "ready") return;

    if (!auth.user) {
      navigate("Login");
      return;
    }

    loadJobs({ merge: false });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth.status, auth.user, auth.token]);

  React.useEffect(() => {
    if (auth.status !== "ready" || !auth.user) return;
    if (!isSuperuser) return;
    if (tab !== "admin") return;

    loadScheduler();
    loadCleanup();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, auth.status, auth.user, auth.token, isSuperuser]);

  React.useEffect(() => {
    if (auth.status !== "ready" || !auth.user) return;
    if (!isSuperuser) return;

    loadUsers();
    loadAdminSites();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth.status, auth.user, auth.token, isSuperuser]);

  React.useEffect(() => {
    if (!isSuperuser) return;

    if (!selectedUserId) {
      setSelectedUser(null);
      setSitePermissions({});
      return;
    }

    const user = allUsers.find((u) => String(u.id) === String(selectedUserId)) || null;
    setSelectedUser(user);
    loadUserPermissions(selectedUserId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedUserId, isSuperuser, allUsers]);

  async function refreshStatus() {
    if (!auth.user) return;

    setLoadingJobs(true);
    setErr("");

    try {
      await api.accountRefresh(auth.token);
      await loadJobs({ merge: true });
    } catch (ex) {
      setErr(ex.message || String(ex));
    } finally {
      setLoadingJobs(false);
    }
  }

  React.useEffect(() => {
    if (auth.status !== "ready" || !auth.user) return;

    const hasPending = (jobs || []).some((j) => {
      const s = String(j.status || "").toLowerCase();
      return s === "queued" || s === "running";
    });

    if (!hasPending) return;

    const timer = window.setInterval(async () => {
      try {
        await api.accountRefresh(auth.token);
        await loadJobs({ merge: true });
      } catch {
        // keep silent
      }
    }, 5000);

    return () => window.clearInterval(timer);
  }, [auth.status, auth.user, auth.token, jobs]);

  async function view(runLikeObj) {
    setActiveJob(runLikeObj);
    setOpen(true);

    setResult(null);
    setManifest(null);
    setSelModel("");
    setSelTreatment("");
    setVariable("GPP");

    setLoadingRun(true);

    try {
      const man = await api.workflowRunManifest(auth.token, runLikeObj.id);
      setManifest(man);

      const models = deriveModels(man);
      const treatments = deriveTreatments(man);

      const model0 = models[0] || "";
      const treatment0 = treatments[0] || "";

      setSelModel(model0);
      setSelTreatment(treatment0);

      const vars = deriveVariables(man, model0, treatment0);
      const v0 = vars.includes("GPP") ? "GPP" : (vars[0] || "GPP");
      setVariable(v0);

      const r = await api.workflowRunTimeseries(auth.token, runLikeObj.id, {
        variable: v0,
        model: model0,
        treatment: treatment0,
      });

      setResult(r);
    } catch (ex) {
      alert(ex.message || String(ex));
    } finally {
      setLoadingRun(false);
    }
  }

  async function reloadTimeseries(next = {}) {
    if (!activeJob) return;

    const v = next.variable ?? variable;
    const m = next.model ?? selModel;
    const t = next.treatment ?? selTreatment;

    setLoadingRun(true);

    try {
      const r = await api.workflowRunTimeseries(auth.token, activeJob.id, {
        variable: v,
        model: m,
        treatment: t,
      });
      setResult(r);
    } catch (ex) {
      alert(ex.message || String(ex));
    } finally {
      setLoadingRun(false);
    }
  }

  return e(
    "div",
    { className: "panel" },

    e(
      "div",
      { className: "section-head" },
      e("h2", null, "Account"),
      e(
        "div",
        null,
        tab === "jobs"
          ? e(
              "button",
              {
                type: "button",
                className: "btn",
                onClick: refreshStatus,
                disabled: loadingJobs,
              },
              loadingJobs ? "Refreshing..." : "Refresh status"
            )
          : null,
        e(
          "button",
          {
            type: "button",
            className: "btn",
            onClick: logout,
            style: { marginLeft: 8 },
          },
          "Log out"
        )
      )
    ),

    err ? e("div", { className: "error", style: { marginTop: 8 } }, err) : null,

    e(
      "div",
      { className: "muted" },
      auth.user ? `User: ${auth.user.username} (${auth.user.role})` : ""
    ),

    e(
      "div",
      { className: "chips", style: { marginTop: 14, marginBottom: 8 } },
      e(TabChip, { active: tab === "jobs", onClick: () => setTab("jobs") }, "Jobs"),
      e(TabChip, { active: tab === "schedules", onClick: () => setTab("schedules") }, "Schedules"),
      isSuperuser
        ? e(TabChip, { active: tab === "admin", onClick: () => setTab("admin") }, "Admin")
        : null
    ),

    tab === "jobs"
      ? e(AccountJobsPanel, {
          auth,
          jobs,
          loadingRun,
          onView: view,
          onRefreshJobs: () => loadJobs({ merge: false }),
        })
      : null,

    tab === "schedules"
      ? e(AccountSchedulesPanel, {
          auth,
          onViewRun: view,
        })
      : null,

    tab === "admin" && isSuperuser
      ? e(AccountAdminPanel, {
          auth,
          allUsers,
          usersLoading,
          usersError,
          adminSites,
          sitesLoading,
          sitesError,
          selectedUserId,
          setSelectedUserId,
          selectedUser,
          sitePermissions,
          permLoading,
          permSaving,
          permError,
          permNotice,
          updateSitePermission,
          savePermissions,

          schedulerStatus,
          schedulerLoading,
          schedulerError,
          schedulerNotice,
          loadScheduler,
          reloadSchedulerJobs,

          cleanupCandidates,
          cleanupLogs,
          cleanupLoading,
          cleanupError,
          cleanupNotice,
          ttlEphemeral,
          setTtlEphemeral,
          ttlNormal,
          setTtlNormal,
          loadCleanup,
          cleanupDryRun,
          cleanupRun,
        })
      : null,

    e(AccountJobViewerModal, {
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
      onClose: () => setOpen(false),
    })
  );
}