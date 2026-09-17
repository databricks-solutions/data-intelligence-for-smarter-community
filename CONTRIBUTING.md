# Contributing

Thanks for your interest in improving **Data Intelligence for Smarter Communities** — a
hands-on Databricks accelerator that takes generic telco network signal + campus IoT data
end-to-end on the Databricks Data Intelligence Platform (Bronze → Silver → Gold → AI/BI →
Genie → Databricks App), using the UBC Vancouver campus as a real geospatial testbed.

## What this repo is

An end-to-end teaching/demo asset for smart-community use cases (transit, congestion,
safety, infrastructure utilization). It is meant to run on any fresh, admin-owned Databricks
workspace (Free Edition / Vocareum / FEVM) and to be reusable by any account team as a
generic, telco-provider-neutral accelerator.

## Data policy (required)

- **Synthetic data only.** All telemetry in this repo is synthetic, generated at runtime by
  `workshop/Includes/smart_campus_generators.py` and modeled on publicly-known patterns.
- **No customer data, PII, or proprietary information.** Do not add any real network data,
  customer records, or data derived from a specific carrier's proprietary systems.
- **No credentials, tokens, or passwords** in code, notebooks, or config.
- **Provider-neutral.** Keep the content generic — do not introduce specific telco / ISP
  brand names, real PLMNs, or real network identifiers. Use the example test PLMN (`001-01`)
  and generic labels ("the telco", "the carrier", "broadband gateway").
- The only real data is **UBCGeodata** (building + neighbourhood footprints), used under
  **CC BY 4.0** — keep its attribution intact.

## Conventions

- **Naming:** lowercase with hyphens for files/folders; `snake_case` for tables and columns;
  the smart-community Bronze tables use the neutral `sc_` prefix (e.g. `sc_access_events`).
- Keep notebooks runnable top-to-bottom on serverless; don't pin library versions or add
  cluster/setup requirements unless necessary.
- Match the style, comment density, and structure of the surrounding notebooks and app code.

## Submitting changes

1. Create a branch and make your change.
2. Verify the affected labs run end-to-end on a clean workspace.
3. Open a Pull Request using the PR template, describe the change and how you validated it,
   and confirm the data policy above.
4. Report bugs or request enhancements via GitHub Issues using the provided templates.

See [NOTICE.md](NOTICE.md) for support expectations and [SECURITY.md](SECURITY.md) for
reporting security vulnerabilities.
