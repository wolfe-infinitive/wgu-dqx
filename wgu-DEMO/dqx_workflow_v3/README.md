# WGU DQX Workflow Pipeline

This repository contains a complete three-stage **Data Quality and Enrichment Workflow** using Databricks Labs DQX.

---
## 🚀 Overview

| Stage | Script | Purpose |
|-------|---------|----------|
| 1️⃣ Data Creation | `wgu_data_creation_workflow_1_v3.py` | Generates synthetic student data and inserts it into the Bronze Delta table. |
| 2️⃣ Profiler > DQChecker | `wgu_profiler_dqchecker_2_v3.py` | Profiles, validates, and applies DQX rules to Bronze data → outputs Valid & Quarantined tables. |
| 3️⃣ Data Enrichment | `wgu_data_enrichment_workflow_3_v3.py` | Enriches data with location info, cleans formats, and filters for current students to build Silver/Gold views. |

---
## ⚙️ Execution Order

1. **Run Data Creation** → creates synthetic test data.
2. **Run Profiler > DQChecker** → executes DQX profiling, rule generation, and validation.
3. **Run Data Enrichment** → joins location info and generates clean analytical tables.

---
## 🧩 Key References

- **Databricks Labs DQX GitHub:** [https://github.com/databrickslabs/dqx](https://github.com/databrickslabs/dqx)
- **Official DQX Documentation:** [https://databrickslabs.github.io/dqx/docs/](https://databrickslabs.github.io/dqx/docs/guide/)

## Adhoc jobs
dqx_adhoc_profiler can be ran straight from notebook for adhoc jobs. Can be pointed at table(s) or a file for input

