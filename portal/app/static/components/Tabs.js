import { AppCtx } from "../core/context.js";

export function Tabs({ tabs, active, onChange }) {
  const e = React.createElement;
  const { auth } = React.useContext(AppCtx);

  function handleClick(key) {
    const isProtected = key === "Workflow" || key === "Account";

    if (isProtected && auth.status === "ready" && !auth.user) {
      onChange("Login");
      return;
    }

    onChange(key);
  }

  return e(
    "div",
    { className: "tabs" },
    (tabs || []).map((tab) =>
      e(
        "button",
        {
          key: tab.key,
          type: "button",
          className: "tab" + (active === tab.key ? " active" : ""),
          onClick: () => handleClick(tab.key),
        },
        tab.label
      )
    )
  );
}