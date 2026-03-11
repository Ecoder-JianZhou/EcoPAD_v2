/*
Simple SVG plot with:
- x/y axes
- hover indicator
- hover info box

Assumes:
- x: ISO time string array
- y: numeric array
*/

export function PlotSVG({
  title,
  x,
  y,
  width = 820,
  height = 260,
  color = "rgba(29,78,216,0.9)",
  points = false,
}) {
  const e = React.createElement;
  const pad = 40;

  const xs = Array.isArray(x) ? x : [];
  const ys = Array.isArray(y) ? y : [];
  const n = Math.min(xs.length, ys.length);

  const [hover, setHover] = React.useState(null);

  if (n === 0) {
    return e("div", { className: "muted" }, "No data");
  }

  const xVals = xs.slice(0, n);
  const yVals = ys.slice(0, n);

  const ymin = Math.min(...yVals);
  const ymax = Math.max(...yVals);

  function xToPx(i) {
    return pad + (width - 2 * pad) * (i / Math.max(1, n - 1));
  }

  function yToPx(v) {
    if (ymax === ymin) return height / 2;
    return pad + (height - 2 * pad) * (1 - (v - ymin) / (ymax - ymin));
  }

  const path = yVals
    .map((v, i) => `${i === 0 ? "M" : "L"} ${xToPx(i).toFixed(2)} ${yToPx(v).toFixed(2)}`)
    .join(" ");

  function handleMouseMove(ev) {
    const rect = ev.currentTarget.getBoundingClientRect();
    const mx = ev.clientX - rect.left;
    const t = (mx - pad) / (width - 2 * pad);
    const idx = Math.max(0, Math.min(n - 1, Math.round(t * (n - 1))));
    setHover({
      idx,
      px: xToPx(idx),
      py: yToPx(yVals[idx]),
    });
  }

  return e(
    "div",
    { className: "plotbox" },
    title ? e("div", { className: "plot-title" }, title) : null,

    e(
      "svg",
      {
        className: "plot plot-svg",
        viewBox: `0 0 ${width} ${height}`,
        onMouseMove: handleMouseMove,
        onMouseLeave: () => setHover(null),
      },

      e("line", {
        x1: pad,
        y1: height - pad,
        x2: width - pad,
        y2: height - pad,
        stroke: "rgba(11,18,32,0.35)",
      }),
      e("line", {
        x1: pad,
        y1: pad,
        x2: pad,
        y2: height - pad,
        stroke: "rgba(11,18,32,0.35)",
      }),

      [0, 0.5, 1].map((t, k) => {
        const v = ymin + t * (ymax - ymin);
        const py = yToPx(v);
        return e(
          "g",
          { key: k },
          e("line", {
            x1: pad - 5,
            y1: py,
            x2: pad,
            y2: py,
            stroke: "rgba(11,18,32,0.35)",
          }),
          e(
            "text",
            {
              x: 6,
              y: py + 4,
              fontSize: 10,
              fill: "rgba(11,18,32,0.75)",
            },
            v.toFixed(2)
          )
        );
      }),

      e("path", {
        d: path,
        fill: "none",
        stroke: color,
        strokeWidth: 2,
      }),

      points
        ? yVals.map((v, i) =>
            e("circle", {
              key: i,
              cx: xToPx(i),
              cy: yToPx(v),
              r: 2.2,
              fill: color,
              opacity: 0.9,
            })
          )
        : null,

      hover
        ? e(
            "g",
            null,
            e("line", {
              x1: hover.px,
              y1: pad,
              x2: hover.px,
              y2: height - pad,
              stroke: "rgba(0,0,0,0.15)",
            }),
            e("circle", {
              cx: hover.px,
              cy: hover.py,
              r: 4,
              fill: "rgba(0,0,0,0.65)",
            })
          )
        : null
    ),

    hover
      ? e(
          "div",
          { className: "plot-hoverbox" },
          e("div", null, xVals[hover.idx]),
          e("div", null, Number(yVals[hover.idx]).toFixed(4))
        )
      : null
  );
}