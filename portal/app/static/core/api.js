function authHeaders(token) {
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);

  if (!response.ok) {
    let message = response.statusText || "Request failed";

    try {
      const data = await response.json();
      message = data?.detail || message;
    } catch {
      // keep default message
    }

    throw new Error(message);
  }

  return response.json();
}

function buildQuery(paramsObj = {}) {
  const qs = new URLSearchParams();

  Object.entries(paramsObj).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    qs.set(key, String(value));
  });

  const text = qs.toString();
  return text ? `?${text}` : "";
}

export const api = {
  // -------------------------------------------------------------------
  // Auth
  // -------------------------------------------------------------------
  register(username, password) {
    return requestJson("/api/auth/register", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
  },

  login(username, password) {
    return requestJson("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
  },

  logout(token) {
    return requestJson("/api/auth/logout", {
      method: "POST",
      headers: { ...authHeaders(token) },
    });
  },

  me(token) {
    return requestJson("/api/auth/me", {
      headers: { ...authHeaders(token) },
    });
  },

  authUsers(token) {
    return requestJson("/api/auth/users", {
      headers: { ...authHeaders(token) },
    });
  },

  // -------------------------------------------------------------------
  // Admin
  // -------------------------------------------------------------------
  adminUsers(token) {
    return requestJson("/api/admin/users", {
      headers: { ...authHeaders(token) },
    });
  },

  adminSites(token) {
    return requestJson("/api/admin/sites", {
      headers: { ...authHeaders(token) },
    });
  },

  adminUserPermissions(token, userId) {
    return requestJson(`/api/admin/users/${encodeURIComponent(userId)}/permissions`, {
      headers: { ...authHeaders(token) },
    });
  },

  adminSavePermission(token, userId, payload) {
    return requestJson(`/api/admin/users/${encodeURIComponent(userId)}/permissions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(token),
      },
      body: JSON.stringify(payload),
    });
  },

  // -------------------------------------------------------------------
  // Setup
  // -------------------------------------------------------------------
  setupStatus() {
    return requestJson("/api/setup/status");
  },

  setupInit(username, password) {
    return requestJson("/api/setup/init", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
  },

  // -------------------------------------------------------------------
  // Sites
  // -------------------------------------------------------------------
  sitesList() {
    return requestJson("/api/sites/list");
  },

  siteMeta(siteId) {
    return requestJson(`/api/sites/${encodeURIComponent(siteId)}/meta`);
  },

  siteForcing(siteId, vars) {
    const query = buildQuery({ vars });
    return requestJson(`/api/sites/${encodeURIComponent(siteId)}/forcing${query}`);
  },

  // -------------------------------------------------------------------
  // Workflow
  // -------------------------------------------------------------------
  workflowMeta() {
    return requestJson("/api/workflow/meta");
  },

  workflowSites() {
    return requestJson("/api/workflow/sites");
  },

  workflowSiteMeta(siteId) {
    const query = buildQuery({ site: siteId });
    return requestJson(`/api/workflow/site_meta${query}`);
  },

  workflowParamsMeta(siteId, model = "") {
    const query = buildQuery({ site: siteId, model });
    return requestJson(`/api/workflow/params_meta${query}`);
  },

  workflowPermissionsMe(token) {
    return requestJson("/api/workflow/permissions/me", {
      headers: { ...authHeaders(token) },
    });
  },

  workflowSubmit(token, payload) {
    return requestJson("/api/workflow/submit", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(token),
      },
      body: JSON.stringify(payload),
    });
  },

  workflowJobs(token) {
    return requestJson("/api/workflow/jobs", {
      headers: { ...authHeaders(token) },
    });
  },

  workflowJobDetail(token, jobId) {
    return requestJson(`/api/workflow/jobs/${encodeURIComponent(jobId)}`, {
      headers: { ...authHeaders(token) },
    });
  },

  workflowJobResults(token, jobId, variable) {
    const query = buildQuery({ variable });
    return requestJson(`/api/workflow/jobs/${encodeURIComponent(jobId)}/results${query}`, {
      headers: { ...authHeaders(token) },
    });
  },

  workflowRunManifest(token, runId) {
    return requestJson(`/api/workflow/runs/${encodeURIComponent(runId)}/manifest`, {
      headers: { ...authHeaders(token) },
    });
  },

  workflowRunTimeseries(token, runId, { variable, model, treatment }) {
    const query = buildQuery({ variable, model, treatment });
    return requestJson(`/api/workflow/runs/${encodeURIComponent(runId)}/timeseries${query}`, {
      headers: { ...authHeaders(token) },
    });
  },

  workflowRunParameterSummary(token, runId, { model, treatment }) {
    const query = buildQuery({ model, treatment });
    return requestJson(`/api/workflow/runs/${encodeURIComponent(runId)}/parameter_summary${query}`, {
      headers: { ...authHeaders(token) },
    });
  },

  workflowRunParametersAccepted(token, runId, { model, treatment }) {
    const query = buildQuery({ model, treatment });
    return requestJson(`/api/workflow/runs/${encodeURIComponent(runId)}/parameters_accepted${query}`, {
      headers: { ...authHeaders(token) },
    });
  },

  // -------------------------------------------------------------------
  // Forecast
  // -------------------------------------------------------------------
  forecastSites() {
    return requestJson("/api/forecast/sites");
  },

  forecastMeta(siteId) {
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/meta`);
  },

  forecastData(siteId, variable, models = "", treatments = "") {
    const query = buildQuery({
      variable,
      models,
      treatments,
    });
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/data${query}`);
  },

  forecastSummary(siteId) {
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/summary`);
  },

  forecastObs(siteId, variable, treatments = "") {
    const query = buildQuery({
      variable,
      treatments,
    });
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/obs${query}`);
  },

  forecastParamsMeta(siteId, model = "") {
    const query = buildQuery({ model });
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/params/meta${query}`);
  },

  forecastParamsLatest(siteId, model, treatment, variable) {
    const query = buildQuery({
      model,
      treatment,
      variable,
    });
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/params/latest${query}`);
  },

  forecastParamsHistory(siteId, param, models = "", treatments = "", variable = "GPP") {
    const query = buildQuery({
      param,
      models,
      treatments,
      variable,
    });
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/params/history${query}`);
  },

  forecastParamsHist(siteId, runId, models = "", treatments = "", params = "") {
    const query = buildQuery({
      run_id: runId,
      models,
      treatments,
      params,
    });
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/params/hist${query}`);
  },

  forecastRunParameterSummary(siteId, runId, model, treatment) {
    const query = buildQuery({ model, treatment });
    return requestJson(
      `/api/forecast/${encodeURIComponent(siteId)}/runs/${encodeURIComponent(runId)}/parameter_summary${query}`
    );
  },

  forecastRunParametersAccepted(siteId, runId, model, treatment) {
    const query = buildQuery({ model, treatment });
    return requestJson(
      `/api/forecast/${encodeURIComponent(siteId)}/runs/${encodeURIComponent(runId)}/parameters_accepted${query}`
    );
  },

  forecastRuns(siteId, models = "", treatments = "", variable = "", taskType = "", scheduledTaskId = null, limit = 200) {
    const query = buildQuery({
      models,
      treatments,
      variable,
      task_type: taskType,
      scheduled_task_id: scheduledTaskId,
      limit,
    });
    return requestJson(`/api/forecast/${encodeURIComponent(siteId)}/runs${query}`);
  },

  forecastRunTimeseries(siteId, runId, variable, model, treatment) {
    const query = buildQuery({
      variable,
      model,
      treatment,
    });
    return requestJson(
      `/api/forecast/${encodeURIComponent(siteId)}/runs/${encodeURIComponent(runId)}/timeseries${query}`
    );
  },

  // -------------------------------------------------------------------
  // Scheduler
  // -------------------------------------------------------------------
  schedulerStatus(token) {
    return requestJson("/api/scheduler/status", {
      headers: { ...authHeaders(token) },
    });
  },

  schedulerReload(token) {
    return requestJson("/api/scheduler/reload", {
      method: "POST",
      headers: { ...authHeaders(token) },
    });
  },

  schedulerTasks(token) {
    return requestJson("/api/scheduler/tasks", {
      headers: { ...authHeaders(token) },
    });
  },

  schedulerTask(token, scheduleId) {
    return requestJson(`/api/scheduler/tasks/${encodeURIComponent(scheduleId)}`, {
      headers: { ...authHeaders(token) },
    });
  },

  schedulerTaskRuns(token, scheduleId, limit = 50) {
    const query = buildQuery({ limit });
    return requestJson(`/api/scheduler/tasks/${encodeURIComponent(scheduleId)}/runs${query}`, {
      headers: { ...authHeaders(token) },
    });
  },

  schedulerRunOnce(token, scheduleId) {
    return requestJson(`/api/scheduler/run/${encodeURIComponent(scheduleId)}`, {
      method: "POST",
      headers: { ...authHeaders(token) },
    });
  },

  schedulerCreateTask(token, payload) {
    return requestJson("/api/scheduler/tasks", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(token),
      },
      body: JSON.stringify(payload || {}),
    });
  },

  schedulerUpdateTask(token, scheduleId, payload) {
    return requestJson(`/api/scheduler/tasks/${encodeURIComponent(scheduleId)}`, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(token),
      },
      body: JSON.stringify(payload || {}),
    });
  },

  schedulerEnableTask(token, scheduleId) {
    return requestJson(`/api/scheduler/tasks/${encodeURIComponent(scheduleId)}/enable`, {
      method: "POST",
      headers: { ...authHeaders(token) },
    });
  },

  schedulerDisableTask(token, scheduleId) {
    return requestJson(`/api/scheduler/tasks/${encodeURIComponent(scheduleId)}/disable`, {
      method: "POST",
      headers: { ...authHeaders(token) },
    });
  },

  schedulerDeleteTask(token, scheduleId) {
    return requestJson(`/api/scheduler/tasks/${encodeURIComponent(scheduleId)}`, {
      method: "DELETE",
      headers: { ...authHeaders(token) },
    });
  },

  schedulerTasksMine(token, userId) {
    const query = buildQuery({ created_by_user_id: userId });
    return requestJson(`/api/scheduler/tasks${query}`, {
      headers: { ...authHeaders(token) },
    });
  },

  // -------------------------------------------------------------------
  // Cleanup
  // -------------------------------------------------------------------
  cleanupCandidates(token, params = {}) {
    const query = buildQuery({
      ttl_days_ephemeral: params.ttl_days_ephemeral,
      ttl_days_normal: params.ttl_days_normal,
      site_id: params.site_id,
      limit: params.limit,
    });

    return requestJson(`/api/cleanup/candidates${query}`, {
      headers: { ...authHeaders(token) },
    });
  },

  cleanupDryRun(token, payload = {}) {
    return requestJson("/api/cleanup/dry-run", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(token),
      },
      body: JSON.stringify(payload || {}),
    });
  },

  cleanupRun(token, payload = {}) {
    return requestJson("/api/cleanup/run", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(token),
      },
      body: JSON.stringify(payload || {}),
    });
  },

  cleanupLogs(token, params = {}) {
    const query = buildQuery({
      run_id: params.run_id,
      limit: params.limit,
    });

    return requestJson(`/api/cleanup/logs${query}`, {
      headers: { ...authHeaders(token) },
    });
  },

  // -------------------------------------------------------------------
  // Account
  // -------------------------------------------------------------------
  accountJobs(token) {
    return requestJson("/api/account/jobs", {
      headers: { ...authHeaders(token) },
    });
  },

  accountRefresh(token) {
    return requestJson("/api/account/jobs/refresh", {
      method: "POST",
      headers: { ...authHeaders(token) },
    });
  },

  accountDeleteJob(token, jobId) {
    return requestJson(`/api/account/jobs/${encodeURIComponent(jobId)}`, {
      method: "DELETE",
      headers: { ...authHeaders(token) },
    });
  },
};