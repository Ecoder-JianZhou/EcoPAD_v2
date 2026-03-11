let pushToast = null;

const TOAST_LIFETIME_MS = 2400;

export function toast(message) {
  if (pushToast) {
    pushToast(message);
  }
}

export function ToastHost() {
  const e = React.createElement;
  const [items, setItems] = React.useState([]);

  React.useEffect(() => {
    pushToast = (message) => {
      const id = Math.random().toString(36).slice(2);

      setItems((prev) => [...prev, { id, message }]);

      window.setTimeout(() => {
        setItems((prev) => prev.filter((item) => item.id !== id));
      }, TOAST_LIFETIME_MS);
    };

    return () => {
      pushToast = null;
    };
  }, []);

  return e(
    "div",
    { className: "toast-host" },
    items.map((item) =>
      e("div", { key: item.id, className: "toast" }, item.message)
    )
  );
}