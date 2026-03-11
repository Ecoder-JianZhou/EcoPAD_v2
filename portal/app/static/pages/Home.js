import { AppCtx } from "../core/context.js";

export function Home() {
  const e = React.createElement;
  const { auth, navigate } = React.useContext(AppCtx);

  function go(key) {
    if (key === "forecast") {
      navigate("Forecast");
      return;
    }

    if (key === "workflow") {
      navigate(auth?.user ? "Workflow" : "Login");
      return;
    }

    navigate("Home");
  }

  return e(
    "div",
    { className: "panel" },
    e(
      "div",
      null,
      e(HomeHero, { go }),
      e("div", { style: { height: 12 } }),
      e(HomeSpruceDemo),
      e("div", { style: { height: 12 } }),
      e(HomeModulesTabs)
    )
  );
}

/* ------------------------------------------------------------------ */
/* Hero */
/* ------------------------------------------------------------------ */
function HomeHero({ go }) {
  const e = React.createElement;

  return e(
    "section",
    { className: "home-hero" },

    e(
      "div",
      { className: "home-hero-left" },

      e(
        "p",
        { className: "muted justify" },
        "EcoPAD is an interactive Model–Experiment (ModEx) platform that integrates models and diverse data streams through rigorous data assimilation to improve ecological prediction and quantify uncertainty."
      ),

      e(
        "p",
        { className: "muted justify" },
        "EcoPAD provides continuous feedback between experiments and models: guiding experimental design by identifying critical data needs, and informing model development by revealing structural and parameter limitations. By iteratively assimilating observations, EcoPAD constrains model parameters and system states, updates projections, and improves predictions throughout the experimental process."
      ),

      e(
        "ul",
        { className: "what-list" },
        homeListItem("Predict ecosystem responses to experimental treatments after site selection and experimental design."),
        homeListItem("Assimilate ongoing observations to constrain and update model predictions."),
        homeListItem("Project ecosystem trajectories for the remainder of an experiment."),
        homeListItem("Identify the most informative datasets needed to improve process understanding."),
        homeListItem("Periodically update forecasts as new data become available."),
        homeListItem("Iteratively improve models, data assimilation frameworks, and experimental strategies.")
      ),

      e(
        "div",
        { className: "home-cta" },

        e(
          "button",
          {
            type: "button",
            className: "btn",
            onClick: () => go("forecast"),
          },
          "Explore Forecasts"
        ),

        e(
          "button",
          {
            type: "button",
            className: "btn",
            onClick: () => go("workflow"),
          },
          "Go to Custom Workflow"
        )
      )
    ),

    e(
      "div",
      { className: "home-hero-right" },
      e("img", {
        className: "hero-image",
        src: "/static/img/ecopad_workflow.jpg",
        alt: "EcoPAD workflow",
      })
    )
  );
}

function homeListItem(text) {
  return React.createElement("li", null, text);
}

/* ------------------------------------------------------------------ */
/* SPRUCE demo */
/* ------------------------------------------------------------------ */
function HomeSpruceDemo() {
  const e = React.createElement;

  return e(
    "section",
    { className: "panel demo" },

    e(
      "div",
      { className: "section-head" },
      e("h2", null, "SPRUCE Demo"),
      e(
        "div",
        { className: "section-note muted" },
        "Auto-rotating animations (click left/right to switch)"
      )
    ),

    e(
      "p",
      { className: "muted justify" },
      "This demo illustrates how EcoPAD organizes site-level forecasts and diagnostics for SPRUCE, enabling continuous updates as new observations are assimilated."
    ),

    e(HomeDemoTwoCol)
  );
}

function HomeDemoTwoCol() {
  const e = React.createElement;

  const slides = [
    { key: "gpp", title: "GPP", src: "/static/demo/gpp_forecast.gif" },
    { key: "er", title: "ER", src: "/static/demo/er_forecast.gif" },
    { key: "foliage", title: "Foliage", src: "/static/demo/foliage_forecast.gif" },
    { key: "wood", title: "Wood", src: "/static/demo/wood_forecast.gif" },
    { key: "root", title: "Root", src: "/static/demo/root_forecast.gif" },
    { key: "soil", title: "Soil", src: "/static/demo/soil_forecast.gif" },
    { key: "forcing", title: "Forcing Uncertainty", src: "/static/demo/forcing_uncertainty.gif" },
    { key: "add", title: "Added Data", src: "/static/demo/add_data.gif" },
  ];

  const [index, setIndex] = React.useState(0);

  React.useEffect(() => {
    const timer = window.setInterval(() => {
      setIndex((prev) => (prev + 1) % slides.length);
    }, 6000);

    return () => window.clearInterval(timer);
  }, [slides.length]);

  const current = slides[index];

  function prev() {
    setIndex((index - 1 + slides.length) % slides.length);
  }

  function next() {
    setIndex((index + 1) % slides.length);
  }

  return e(
    "div",
    { className: "demo-grid" },

    e(
      "div",
      { className: "demo-left" },

      e("img", {
        className: "demo-site-img",
        src: "/static/demo/spruce_site.png",
        alt: "SPRUCE site",
      }),

      e(
        "div",
        { className: "demo-site-caption muted" },
        "SPRUCE (Spruce and Peatland Responses Under Changing Environments)"
      )
    ),

    e(
      "div",
      { className: "demo-right" },

      e(
        "div",
        {
          className: "tiff-stage",
          role: "group",
          "aria-label": "Demo animation carousel",
        },

        e("img", {
          className: "gif-image",
          src: current.src,
          alt: current.title,
        }),

        e(
          "button",
          {
            type: "button",
            className: "nav-zone left",
            onClick: prev,
            "aria-label": "Previous",
          },
          e("span", { className: "tri left" })
        ),

        e(
          "button",
          {
            type: "button",
            className: "nav-zone right",
            onClick: next,
            "aria-label": "Next",
          },
          e("span", { className: "tri right" })
        )
      )
    )
  );
}

/* ------------------------------------------------------------------ */
/* Modules */
/* ------------------------------------------------------------------ */
function HomeModulesTabs() {
  const e = React.createElement;

  const tabs = [
    {
      key: "da",
      title: "Data assimilation",
      img: "/static/img/mod_da.jpg",
      text: "EcoPAD applies data assimilation to constrain parameters and system states by integrating observations with models in a rigorous Bayesian framework, improving prediction skill and quantifying uncertainty. (Figure from Huang et al., 2019, GMD)",
    },
    {
      key: "teco",
      title: "TECO model",
      img: "/static/img/mod_teco.jpg",
      text: "TECO is a process-based ecosystem model supporting site-scale carbon and energy flux simulations. EcoPAD organizes TECO configurations, inputs, and outputs into reproducible workflows. (Figure from Weng and Luo, 2008. JGR.)",
    },
    {
      key: "matrix",
      title: "Matrix model",
      img: "/static/img/mod_matrix.jpg",
      text: "Matrix-based representations enable efficient analysis of ecosystem state transitions and carbon pool dynamics, supporting diagnostics and computational acceleration. (Figure from Hou et al., 2023. GCB)",
    },
    {
      key: "trace",
      title: "Traceability analysis",
      img: "/static/img/mod_trace.jpg",
      text: "Traceability analysis attributes emergent ecosystem responses to underlying process components, helping identify key drivers, sensitivities, and model limitations. (Figure from Xia et al., 2020. GCB)",
    },
  ];

  const [active, setActive] = React.useState("da");

  const current = tabs.find((t) => t.key === active) || tabs[0];

  return e(
    "section",
    { className: "panel modules" },

    e(
      "div",
      { className: "section-head" },
      e("h2", null, "Modules"),
      e("div", { className: "section-note muted" }, "Concept overview")
    ),

    e(
      "div",
      { className: "modules-tabs" },
      tabs.map((tab) =>
        e(
          "button",
          {
            key: tab.key,
            type: "button",
            className: "mod-tab" + (tab.key === active ? " active" : ""),
            onClick: () => setActive(tab.key),
          },
          tab.title
        )
      )
    ),

    e(
      "div",
      { className: "modules-body" },
      e("img", {
        className: "modules-img",
        src: current.img,
        alt: current.title,
      }),
      e(
        "div",
        { className: "modules-text" },
        e("h3", null, current.title),
        e("p", { className: "muted justify" }, current.text)
      )
    )
  );
}