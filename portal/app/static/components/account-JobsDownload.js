import { api } from "../core/api.js";
import { Modal } from "./Modal.js";

function artifactFileName(item) {
  const rel = String(item?.rel_path || "").trim();
  if (!rel) return "";
  const parts = rel.split("/");
  return parts[parts.length - 1] || rel;
}

function artifactLabel(item) {
  const parts = [
    item?.artifact_type || "",
    item?.model_id || "",
    item?.treatment || "",
    item?.variable || "",
    item?.output_type || item?.series_type || "",
  ].filter(Boolean);
  return parts.join(" · ");
}

function humanSize(n) {
  const x = Number(n);
  if (!Number.isFinite(x) || x < 0) return "—";
  if (x < 1024) return `${x} B`;
  if (x < 1024 * 1024) return `${(x / 1024).toFixed(1)} KB`;
  if (x < 1024 * 1024 * 1024) return `${(x / (1024 * 1024)).toFixed(1)} MB`;
  return `${(x / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function triggerBrowserDownload(url, filename = "") {
  const a = document.createElement("a");
  a.href = url;
  if (filename) a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

export function AccountJobsDownloadModal({
  auth,
  open,
  job,
  onClose,
}) {
  const e = React.createElement;
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState("");
  const [artifacts, setArtifacts] = React.useState([]);
  const [busyKey, setBusyKey] = React.useState("");

  React.useEffect(() => {
    let cancelled = false;

    if (!open || !job?.id || !auth?.token) {
      setArtifacts([]);
      setError("");
      setLoading(false);
      return;
    }

    setLoading(true);
    setError("");
    setArtifacts([]);

    api.runArtifacts(auth.token, job.id)
      .then((j) => {
        if (cancelled) return;
        setArtifacts(Array.isArray(j?.artifacts) ? j.artifacts : []);
      })
      .catch((ex) => {
        if (cancelled) return;
        setError(ex.message || "Failed to load downloadable files.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [open, auth, job]);

  function downloadManifest() {
    if (!job?.id) return;
    setBusyKey("manifest");
    try {
      const url = `/api/workflow/runs/${encodeURIComponent(job.id)}/manifest`;
      triggerBrowserDownload(url, `run-${job.id}-manifest.json`);
    } finally {
      setTimeout(() => setBusyKey(""), 300);
    }
  }

  function downloadBundle() {
    if (!job?.id) return;
    setBusyKey("bundle");
    try {
      const url = api.runDownloadUrl(job.id, { bundle: true });
      triggerBrowserDownload(url);
    } finally {
      setTimeout(() => setBusyKey(""), 300);
    }
  }

  function downloadArtifact(item, idx) {
    if (!job?.id) return;
    const key = `artifact-${idx}`;
    setBusyKey(key);
    try {
      const url = api.runDownloadUrl(job.id, {
        rel_path: item?.rel_path || "",
      });
      triggerBrowserDownload(url, artifactFileName(item));
    } finally {
      setTimeout(() => setBusyKey(""), 300);
    }
  }

  return e(
    Modal,
    {
      open,
      title: job ? `Downloads · Run ${job.id}` : "Downloads",
      onClose,
      width: 980,
    },

    !job
      ? null
      : e(
          "div",
          null,

          e(
            "div",
            {
              className: "card",
              style: { marginBottom: 12 },
            },
            e(
              "div",
              {
                className: "section-head",
                style: { marginBottom: 10 },
              },
              e(
                "div",
                null,
                e("h3", null, "Download files"),
                e(
                  "div",
                  { className: "muted", style: { marginTop: 4 } },
                  `${job.name || "(unnamed job)"} · #${job.id}`
                )
              ),
              e(
                "div",
                {
                  style: {
                    display: "flex",
                    gap: 8,
                    flexWrap: "wrap",
                  },
                },
                e(
                  "button",
                  {
                    type: "button",
                    className: "btn",
                    onClick: downloadManifest,
                    disabled: busyKey === "manifest",
                  },
                  busyKey === "manifest" ? "Downloading..." : "Manifest"
                ),
                e(
                  "button",
                  {
                    type: "button",
                    className: "btn primary",
                    onClick: downloadBundle,
                    disabled: busyKey === "bundle",
                  },
                  busyKey === "bundle" ? "Downloading..." : "Download all (.zip)"
                )
              )
            )
          ),

          e(
            "div",
            {
              className: "card",
            },
            loading ? e("div", { className: "muted" }, "Loading files...") : null,
            error ? e("div", { className: "muted" }, error) : null,

            !loading && !error && artifacts.length === 0
              ? e("div", { className: "muted" }, "No downloadable artifacts found.")
              : null,

            !loading && artifacts.length > 0
              ? e(
                  "div",
                  {
                    style: {
                      display: "grid",
                      gap: 8,
                    },
                  },
                  ...artifacts.map((item, idx) =>
                    e(
                      "div",
                      {
                        key: `${item.rel_path || ""}-${idx}`,
                        className: "card",
                        style: {
                          padding: 10,
                          border: "1px solid rgba(0,0,0,0.06)",
                        },
                      },
                      e(
                        "div",
                        {
                          style: {
                            display: "flex",
                            justifyContent: "space-between",
                            gap: 12,
                            alignItems: "flex-start",
                          },
                        },
                        e(
                          "div",
                          { style: { minWidth: 0, flex: "1 1 auto" } },
                          e(
                            "div",
                            {
                              style: {
                                fontWeight: 600,
                                wordBreak: "break-word",
                              },
                            },
                            artifactFileName(item) || item.rel_path || "(unnamed artifact)"
                          ),
                          e(
                            "div",
                            {
                              className: "muted",
                              style: {
                                marginTop: 4,
                                wordBreak: "break-word",
                              },
                            },
                            artifactLabel(item) || "Artifact"
                          ),
                          e(
                            "div",
                            {
                              className: "muted",
                              style: {
                                marginTop: 4,
                                fontSize: 12,
                                wordBreak: "break-word",
                              },
                            },
                            `${item.rel_path || "—"} · ${humanSize(item.size)}`
                          )
                        ),
                        e(
                          "div",
                          { style: { flex: "0 0 auto" } },
                          e(
                            "button",
                            {
                              type: "button",
                              className: "btn",
                              onClick: () => downloadArtifact(item, idx),
                              disabled:
                                busyKey === `artifact-${idx}` || item.exists === false,
                            },
                            busyKey === `artifact-${idx}` ? "Downloading..." : "Download"
                          )
                        )
                      )
                    )
                  )
                )
              : null
          )
        )
  );
}