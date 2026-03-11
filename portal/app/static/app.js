import { AppProvider, AppCtx } from "./core/context.js";
import { Tabs } from "./components/Tabs.js";
import { ToastHost } from "./components/Toast.js";

import { Home } from "./pages/Home.js";
import { Login } from "./pages/Login.js";
import { Forecast } from "./pages/Forecast.js";
import { Workflow } from "./pages/Workflow.js";
import { Account } from "./pages/Account.js";


const e = React.createElement;

const PAGE_COMPONENTS = {
  Home,
  Login,
  Forecast,
  Workflow,
  Account,
};


function buildTabs(auth) {
  const accountLabel = auth?.user?.username
    ? `@${auth.user.username}`
    : "Log in";

  return [
    { key: "Home", label: "Home" },
    { key: "Forecast", label: "Forecast" },
    { key: "Workflow", label: "Custom Workflow" },
    { key: "Account", label: accountLabel },
  ];
}


function AppShell({ activePage, setActivePage }) {
  const { auth } = React.useContext(AppCtx);

  const tabs = React.useMemo(
    () => buildTabs(auth),
    [auth?.user?.username]
  );

  function handleTabChange(nextPage) {
    if (nextPage === "Account" && !auth?.user) {
      setActivePage("Login");
      return;
    }
    setActivePage(nextPage);
  }

  const ActiveComponent = PAGE_COMPONENTS[activePage] || Home;

  return e("div", { className: "app-shell" },

    e("header", { className: "topbar" },
      e("div", { className: "header-inner" },

        e("div", { className: "brand" },
          e("div", { className: "logo-box" },
            e("img", {
              className: "logo",
              src: "/static/img/logo5.png",
              alt: "EcoPAD"
            })
          )
        ),

        e("div", { className: "topbar-right" },
          e(Tabs, {
            tabs,
            active: activePage,
            onChange: handleTabChange
          })
        )
      )
    ),

    e("main", { className: "container" },
      e(ActiveComponent)
    ),

    e("footer", { className: "site-footer" },
      e("div", { className: "muted" }, `© ${new Date().getFullYear()}`),
      e("div", { className: "footer-logos" },
        e("img", {
          className: "footer-logo",
          src: "/static/img/ecolab.png",
          alt: "EcoLab"
        }),
        e("img", {
          className: "footer-logo",
          src: "/static/img/cals.png",
          alt: "Cornell CALS"
        })
      )
    ),

    e(ToastHost)
  );
}


function App() {
  const [activePage, setActivePage] = React.useState("Home");

  return e(
    AppProvider,
    { navigate: setActivePage },
    e(AppShell, {
      activePage,
      setActivePage,
    })
  );
}


const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(e(App));