# Data Intelligence for Smarter Communities

A hands-on Databricks accelerator that shows how a **telco provider** can turn network
signal + campus/city IoT data into governed **smart-community intelligence** on the
Databricks Data Intelligence Platform — an alternative to communities building an in-house
data lake. You start with synthetic **smart-community data** (cellular-tower + IoT signals)
and take it end-to-end — **generate → clean → visualize → ask in plain English → ship an
app** — building toward **transit**, **congestion**, and **security** use cases.

The **UBC Vancouver campus** is the testbed: telemetry is synthetic, but it's grounded on
real UBC building footprints so the map and insights feel real. By the end you'll have a
live **Databricks App**: an interactive campus map over your own data, with an **Ask Genie**
panel.

## What you'll build

The full **Data Intelligence Platform loop** on smart-community data:

**Bronze** (generated telemetry) → **Silver** (cleaned + fused with real UBC geodata) →
**Gold** (serving tables) → **AI/BI** dashboards → **Genie** natural-language Q&A →
**Databricks App** (interactive campus map + Ask Genie).

---

## ✅ Prerequisites

You need a **Databricks workspace where you are an admin** — e.g. **Databricks Free Edition**
or a **Vocareum** lab account. Specifically:

- **Serverless** compute available (SQL warehouse + serverless notebooks)
- Permission to **create a Unity Catalog catalog** (the setup creates `labuser_<your-username>`)
- **Genie** and **Databricks Apps** enabled

> Don't run this in a shared/production workspace — the labs create catalogs, schemas,
> tables, a Genie space, and an App under your user.

---

## 🚀 Get started (3 steps)

**1. Clone this repo into your workspace as a Git folder**
- In Databricks: **Workspace** (left sidebar) → your home → **Create → Git folder**
- Git repository URL: `https://github.com/databricks-solutions/data-intelligence-for-smarter-community`
- Branch: **`main`** → **Create Git folder**

**2. Open the course and attach Serverless**
- Go to the `workshop/` folder → open **`00 - Course Overview`**
- Click **Connect** (top-right) → **Serverless**

**3. Run the labs in order (00 → 06)**
- Run each notebook top to bottom. The first lab's setup cell provisions your data
  (~2 min, one time). Each lab builds on the previous one.

---

## 🧭 The labs

| # | Notebook | You build |
|---|---|---|
| 00 | Course Overview | Agenda + requirements |
| 01 | Explore & Profile the Data | Query + profile the raw data in Unity Catalog |
| 02 | Ingest Geodata & Clean | Load real UBC map data → **Silver** tables |
| 03 | Visualize with AI/BI | An **AI/BI dashboard** (transit + security) |
| 04 | Build Serving Tables | **Gold** tables for the app + Genie |
| 05 | Create a Genie Space | Ask your data questions in plain English |
| 06 | Deploy the App | Your live campus-map **Databricks App** |

---

## 📎 Notes

- **Lab 05 (Genie)** has a couple of UI steps — you create the Genie space in the UI and
  paste its ID into Lab 06. Everything else runs from the notebooks.
- The full course lives in [`workshop/`](workshop/) — see its
  [README](workshop/README.md) for the folder layout.

## Data

- **Synthetic telco + IoT telemetry** — generated at runtime by
  [`workshop/Includes/smart_campus_generators.py`](workshop/Includes/smart_campus_generators.py):
  access events, device health, mobility/presence, wireless cells / coverage / KPIs, gateway
  connectivity, and daily campus metrics. Modeled on publicly-known patterns — **not derived
  from any carrier's proprietary data**. Uses the example test PLMN `001-01`.
- **Real geospatial data** — building footprints and neighbourhoods from **UBCGeodata**
  (UBC Campus & Community Planning) via the Abacus Data Network, used under **CC BY 4.0**.
  This is the only real data and drives the campus map.

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub
issue and the team will help on a best-effort basis. See [NOTICE.md](NOTICE.md) for the
support policy and [SECURITY.md](SECURITY.md) for reporting security issues.

## License

&copy; 2026 Databricks, Inc. All rights reserved. The source in this project is provided
subject to the Databricks License [https://databricks.com/db-license-source] — see
[LICENSE.md](LICENSE.md). All included or referenced third-party libraries and data are
subject to the licenses set forth below.

| library / data | description | license | source |
|----------------|-------------|---------|--------|
| UBCGeodata | UBC Vancouver building footprints & neighbourhood boundaries (the only real data; drives the campus map) | CC BY 4.0 | UBC Campus & Community Planning, via the Abacus Data Network |
| PySpark | Distributed data processing | Apache 2.0 | https://github.com/apache/spark |
| pandas | Data generation / manipulation | BSD-3-Clause | https://github.com/pandas-dev/pandas |
| NumPy | Synthetic data generation | BSD-3-Clause | https://github.com/numpy/numpy |
| FastAPI | Reference app backend | MIT | https://github.com/tiangolo/fastapi |
| React | Reference app frontend | MIT | https://github.com/facebook/react |
| Leaflet | Interactive campus map | BSD-2-Clause | https://github.com/Leaflet/Leaflet |
