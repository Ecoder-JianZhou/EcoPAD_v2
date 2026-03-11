export function Modal({ open, title, onClose, children, width = 920 }) {
  const e = React.createElement;

  React.useEffect(() => {
    if (!open) return;

    function onKeyDown(ev) {
      if (ev.key === "Escape") {
        onClose && onClose();
      }
    }

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open, onClose]);

  if (!open) return null;

  return e(
    "div",
    {
      className: "modal-backdrop",
      onMouseDown: () => onClose && onClose(),
    },
    e(
      "div",
      {
        className: "modal",
        style: { maxWidth: width },
        role: "dialog",
        "aria-modal": "true",
        "aria-label": title || "Modal",
        onMouseDown: (ev) => ev.stopPropagation(),
      },
      e(
        "div",
        { className: "modal-head" },
        e("div", { className: "modal-title" }, title || ""),
        e(
          "button",
          {
            type: "button",
            className: "icon-btn",
            onClick: () => onClose && onClose(),
            title: "Close",
            "aria-label": "Close",
          },
          "×"
        )
      ),
      e("div", { className: "modal-body" }, children)
    )
  );
}