import { AppCtx } from "../core/context.js";

export function Login() {
  const e = React.createElement;
  const { auth, login, register } = React.useContext(AppCtx);

  const [mode, setMode] = React.useState("login"); // login | register
  const [username, setUsername] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [busy, setBusy] = React.useState(false);
  const [err, setErr] = React.useState("");

  async function handleSubmit() {
    setErr("");
    setBusy(true);

    try {
      const u = (username || "").trim();
      const p = password || "";

      if (!u) {
        throw new Error("Username is required.");
      }
      if (!p) {
        throw new Error("Password is required.");
      }

      if (mode === "register") {
        await register(u, p);
        setMode("login");
        setPassword("");
      } else {
        await login(u, p);
      }
    } catch (ex) {
      setErr(ex.message || "Error");
    } finally {
      setBusy(false);
    }
  }

  function onKeyDown(ev) {
    if (ev.key === "Enter" && !busy) {
      handleSubmit();
    }
  }

  return e(
    "div",
    { className: "panel" },

    e(
      "div",
      { className: "section-head" },
      e("h2", null, mode === "register" ? "Register" : "Log in"),
      e(
        "div",
        { className: "muted" },
        auth?.user ? `Logged in as ${auth.user.username}` : ""
      )
    ),

    e(
      "div",
      { className: "card form-card" },

      e(
        "div",
        { className: "muted" },
        "This demo stores users in the Portal container (SQLite)."
      ),

      e(
        "div",
        { className: "form-row" },
        e("label", null, "Username"),
        e("input", {
          value: username,
          onChange: (ev) => setUsername(ev.target.value),
          onKeyDown,
          placeholder: "e.g., guest",
          autoComplete: "username",
        })
      ),

      e(
        "div",
        { className: "form-row" },
        e("label", null, "Password"),
        e("input", {
          type: "password",
          value: password,
          onChange: (ev) => setPassword(ev.target.value),
          onKeyDown,
          placeholder: ">= 6 characters",
          autoComplete: mode === "register" ? "new-password" : "current-password",
        })
      ),

      err ? e("div", { className: "error" }, err) : null,

      e(
        "div",
        { className: "form-actions" },

        e(
          "button",
          {
            type: "button",
            className: "btn primary",
            onClick: handleSubmit,
            disabled: busy,
          },
          busy
            ? "Working..."
            : (mode === "register" ? "Create account" : "Log in")
        ),

        e(
          "button",
          {
            type: "button",
            className: "btn",
            onClick: () => {
              setMode(mode === "register" ? "login" : "register");
              setErr("");
            },
          },
          mode === "register" ? "Switch to login" : "Switch to register"
        )
      )
    )
  );
}