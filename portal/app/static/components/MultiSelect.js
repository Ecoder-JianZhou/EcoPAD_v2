/*
Simple chip-based multi-select.

Props:
- options: string[]
- selected: string[]
- onChange: function(nextArray)
- max: maximum items selectable
*/

export function MultiSelect({ options, selected, onChange, max = 9 }) {
  const e = React.createElement;

  const opts = Array.isArray(options) ? options : [];
  const picked = Array.isArray(selected) ? selected : [];

  function toggleValue(value) {
    const hasValue = picked.includes(value);

    if (hasValue) {
      onChange(picked.filter((x) => x !== value));
      return;
    }

    if (picked.length >= max) return;
    onChange([...picked, value]);
  }

  return e(
    "div",
    { className: "chips" },
    opts.map((value) =>
      e(
        "button",
        {
          key: value,
          type: "button",
          className: "chip" + (picked.includes(value) ? " active" : ""),
          onClick: () => toggleValue(value),
          title: value,
        },
        value
      )
    )
  );
}