function fmtIsoCompact(value) {
  if (!value || typeof value !== "string") return "—";

  const m = value.match(
    /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})?$/
  );
  if (!m) return value;

  const date = m[1];
  const time = m[2];
  const tz = m[3] || "";
  const tz2 = tz === "Z" ? "+00:00" : tz;

  return tz2 ? `${date} ${time} ${tz2}` : `${date} ${time}`;
}

export function AccountAdminPanel(props) {
  const e = React.createElement;

  return e(
    "div",
    { style: { marginTop: 12 } },

    e(
      "div",
      { className: "card" },

      e(
        "div",
        { className: "section-head" },
        e("h3", null, "Admin Controls"),
        e("div", { className: "muted" }, "Grant or revoke site-level Auto Forecast permission.")
      ),

      props.usersError ? e("div", { className: "error", style: { marginBottom: 8 } }, props.usersError) : null,
      props.sitesError ? e("div", { className: "error", style: { marginBottom: 8 } }, props.sitesError) : null,
      props.permError ? e("div", { className: "error", style: { marginBottom: 8 } }, props.permError) : null,
      props.permNotice ? e("div", { className: "admin-notice" }, props.permNotice) : null,

      e(
        "div",
        { className: "admin-layout" },

        e(
          "div",
          { className: "admin-card" },
          e("div", { className: "admin-card-title" }, "User"),

          e(
            "div",
            { className: "ctrl" },
            e("label", null, "User"),
            e(
              "select",
              {
                className: "admin-user-select",
                value: props.selectedUserId,
                onChange: (ev) => props.setSelectedUserId(ev.target.value),
                disabled: props.usersLoading || props.allUsers.length === 0,
              },
              props.allUsers
                .filter((u) => String(u.id) !== String(props.auth.user?.id))
                .map((u) =>
                  e("option", { key: u.id, value: u.id }, `${u.username} (${u.role})`)
                )
            )
          ),

          props.selectedUser
            ? e(
                "div",
                { className: "muted", style: { marginTop: 10 } },
                `Selected user: ${props.selectedUser.username} (${props.selectedUser.role})`
              )
            : e("div", { className: "muted", style: { marginTop: 10 } }, "No user selected.")
        ),

        e(
          "div",
          { className: "admin-card" },
          e("div", { className: "admin-card-title" }, "Site permissions"),

          props.usersLoading || props.sitesLoading || props.permLoading
            ? e("div", { className: "muted" }, "Loading permissions...")
            : (props.adminSites.length === 0
                ? e("div", { className: "muted" }, "No sites available.")
                : e(
                    "div",
                    null,

                    e(
                      "div",
                      { className: "admin-site-table" },

                      e("div", { className: "admin-site-head" },
                        e("div", null, "Site"),
                        e("div", null, "Auto Forecast")
                      ),

                      props.adminSites.map((siteId) => {
                        const row = props.sitePermissions?.[siteId] || {
                          can_auto_forecast: false,
                        };

                        return e(
                          "div",
                          {
                            key: siteId,
                            className: "admin-site-row",
                          },

                          e("div", { className: "admin-site-cell admin-site-cell-name" }, siteId),

                          e("div", { className: "admin-site-cell admin-site-cell-check" },
                            e("label", { className: "admin-check-row" },
                              e("input", {
                                type: "checkbox",
                                checked: !!row.can_auto_forecast,
                                disabled: !props.selectedUserId || props.permSaving,
                                onChange: (ev) => props.updateSitePermission(siteId, ev.target.checked),
                              }),
                              e("span", null, row.can_auto_forecast ? "Enabled" : "Disabled")
                            )
                          )
                        );
                      })
                    ),

                    e(
                      "div",
                      { className: "admin-actions" },
                      e(
                        "button",
                        {
                          type: "button",
                          className: "btn primary",
                          onClick: props.savePermissions,
                          disabled: !props.selectedUserId || props.permSaving,
                        },
                        props.permSaving ? "Saving..." : "Save permissions"
                      )
                    )
                  )
              )
        )
      )
    ),

    e(
      "div",
      { className: "admin-card", style: { marginTop: 16 } },

      e(
        "div",
        { className: "section-head" },
        e("h3", null, "Cleanup management"),
        e(
          "div",
          null,
          e(
            "button",
            {
              type: "button",
              className: "btn",
              onClick: props.loadCleanup,
              disabled: props.cleanupLoading,
            },
            props.cleanupLoading ? "Loading..." : "Refresh"
          )
        )
      ),

      props.cleanupError ? e("div", { className: "error", style: { marginBottom: 8 } }, props.cleanupError) : null,
      props.cleanupNotice ? e("div", { className: "admin-notice", style: { marginBottom: 8 } }, props.cleanupNotice) : null,

      e(
        "div",
        {
          style: {
            display: "flex",
            gap: 12,
            alignItems: "end",
            flexWrap: "wrap",
            marginBottom: 14,
          },
        },

        e(
          "div",
          { className: "ctrl" },
          e("label", null, "TTL ephemeral (days)"),
          e("input", {
            type: "number",
            min: 1,
            value: props.ttlEphemeral,
            onChange: (ev) => props.setTtlEphemeral(ev.target.value),
            style: { width: 120 },
          })
        ),

        e(
          "div",
          { className: "ctrl" },
          e("label", null, "TTL normal (days)"),
          e("input", {
            type: "number",
            min: 1,
            value: props.ttlNormal,
            onChange: (ev) => props.setTtlNormal(ev.target.value),
            style: { width: 120 },
          })
        ),

        e(
          "button",
          {
            type: "button",
            className: "btn",
            onClick: props.cleanupDryRun,
            disabled: props.cleanupLoading,
          },
          "Dry run"
        ),

        e(
          "button",
          {
            type: "button",
            className: "btn primary",
            onClick: props.cleanupRun,
            disabled: props.cleanupLoading,
          },
          "Run cleanup"
        )
      ),

      e("h4", null, "Candidates / last dry-run result"),
      props.cleanupCandidates.length === 0
        ? e("div", { className: "muted", style: { marginBottom: 14 } }, "No cleanup candidates.")
        : e(
            "div",
            { className: "admin-site-table", style: { marginBottom: 18 } },

            e("div", { className: "admin-site-head" },
              e("div", null, "Run"),
              e("div", null, "Info")
            ),

            ...props.cleanupCandidates.map((row, idx) =>
              e(
                "div",
                { key: row.run_id || row.id || idx, className: "admin-site-row", style: { alignItems: "flex-start" } },
                e("div", { className: "admin-site-cell admin-site-cell-name" }, row.run_id || row.id || "—"),
                e(
                  "div",
                  { className: "admin-site-cell", style: { display: "block", width: "100%" } },
                  row.reason ? e("div", null, `Reason: ${row.reason}`) : null,
                  row.run_outputs_to_delete != null
                    ? e("div", { className: "muted", style: { marginTop: 4 } }, `run_outputs_to_delete: ${row.run_outputs_to_delete}`)
                    : null,
                  row.deleted_run_outputs != null
                    ? e("div", { className: "muted", style: { marginTop: 4 } }, `deleted_run_outputs: ${row.deleted_run_outputs}`)
                    : null,
                  row.cleaned != null
                    ? e("div", { className: "muted", style: { marginTop: 4 } }, `cleaned: ${row.cleaned ? "yes" : "no"}`)
                    : null
                )
              )
            )
          ),

      e("h4", null, "Cleanup logs"),
      props.cleanupLogs.length === 0
        ? e("div", { className: "muted" }, "No cleanup logs.")
        : e(
            "div",
            { className: "admin-site-table" },

            e("div", { className: "admin-site-head" },
              e("div", null, "Run"),
              e("div", null, "Log")
            ),

            ...props.cleanupLogs.map((row) =>
              e(
                "div",
                { key: row.id, className: "admin-site-row", style: { alignItems: "flex-start" } },
                e("div", { className: "admin-site-cell admin-site-cell-name" }, row.run_id || "—"),
                e(
                  "div",
                  { className: "admin-site-cell", style: { display: "block", width: "100%" } },
                  e("div", null, row.action || "—"),
                  e("div", { className: "muted", style: { marginTop: 4 } }, row.detail || "—"),
                  e("div", { className: "muted", style: { marginTop: 4 } }, fmtIsoCompact(row.created_at))
                )
              )
            )
          )
    )
  );
}