# DP-700 Notes: Implementing Data Engineering Solutions Using Microsoft Fabric

Cumulative notes, built up as we work through hands-on exercises against the
**Contoso Retail** case study in a live Fabric tenant. Each module below maps
to a DP-700 exam area; entries get added as we complete the matching
exercise, not written speculatively ahead of doing the work.

---

## Module 1: Fabric Workspaces & Lakehouse Fundamentals

### Workspace
- A **workspace** is Fabric's unit of collaboration and access control — a
  container that groups related items (Lakehouses, notebooks, pipelines,
  Power BI reports, etc.) under one set of permissions and one **capacity**
  (the compute/billing unit backing the workspace).
- Exam-relevant: workspace roles (Admin, Member, Contributor, Viewer) govern
  who can create/edit/run items vs. just view them. Workspace-level access
  does not automatically grant item-level access inside a Lakehouse (that's
  governed separately via OneLake data access roles).

### Lakehouse
- A **Lakehouse** is a Fabric item combining:
  - `Files/` — unmanaged storage, any file type (JSON, CSV, Parquet...),
    organized however you like. This is where raw/bronze source data lands.
  - `Tables/` — managed **Delta Lake** tables. Anything written here is
    automatically registered in Fabric's SQL endpoint and discoverable by
    Power BI, Spark, and T-SQL alike.
- Both `Files/` and `Tables/` are backed by the same underlying storage:
  **OneLake**.

### OneLake
- OneLake is Fabric's single, tenant-wide logical data lake (think "OneDrive
  for data"). Every workspace gets a OneLake location automatically — no
  separate storage account to provision.
- Key exam point: because every engine (Spark, SQL, Power BI, KQL) reads the
  same OneLake files in the open **Delta Parquet** format, you don't
  copy/ETL data between engines just to use a different tool on it. A
  notebook writes a Delta table once; the SQL endpoint and Power BI see it
  immediately.
- `spark.read.json("Files/raw/...")` and `df.write.saveAsTable(...)` inside a
  Fabric notebook are *relative to the Lakehouse's OneLake location* — this
  is why there's no connection string or mount step, unlike attaching
  external ADLS storage.

### Medallion architecture (bronze/silver/gold), as applied here
- **Bronze**: raw files as received, `Files/raw/` — `sales_raw.json`,
  `customers.json`, `products.json`, uncleaned.
- **Silver**: validated, typed, deduplicated — `Tables/Sales`,
  `Tables/Products`, `Tables/Customers`, produced by
  `clean_sales_data_fabric.ipynb`.
- **Gold** (not yet built): curated, business-level aggregates for reporting
  — to be added once silver is in place and validated.

### Cleaning notebook: what each step teaches
1. **Read bronze JSON from `Files/raw/`** — schema inferred by
   `spark.read.json`, no connection string needed (OneLake-relative path).
2. **Dedup via window function** — `Window.partitionBy("SalesID")
   .orderBy(desc("LoadTimestamp"))` + `row_number() == 1` keeps the latest
   row per key. Same pattern fixes both an exact duplicate (`SalesID 1005`)
   and a correction where a later row has an updated `Amount`
   (`SalesID 1002`) — "keep latest per key" is the general rule.
3. **Standardize dates/casing/types** — `coalesce(to_date(...), to_date(...))`
   tries multiple date formats in order; `initcap()` fixes casing
   inconsistency; `cast("double")` normalizes a string-typed numeric column.
   A genuinely missing value (`ProductID`) is left `NULL` and flagged, not
   dropped or guessed at.
4. **Late-arriving data detection** — compare `SaleDate` (business date) to
   `LoadTimestamp` (arrival date); a gap > 1 day flags a row that must still
   be filed under its original business date, not the day it was loaded.
5. **Write managed Delta tables** — `saveAsTable` lands `Sales`, `Products`,
   `Customers` in `Tables/`, queryable immediately from the SQL endpoint,
   Power BI Direct Lake, or another notebook.
6. **Cross-check against the Warehouse version** — reproduces the same
   "sales by region" aggregation as DataFrame `groupBy` that Session 1 did
   in T-SQL, joined against the *customer's* region (not the messy
   per-transaction `Region` field) — getting the same answer two ways
   validates the cleaning logic.

### Exercise 1 status
- [x] Lakehouse (`SalesLakehouse`) created in **My workspace**
- [x] `sales_raw.json`, `customers.json`, `products.json` uploaded to
      `Files/raw/`
- [ ] Run `clean_sales_data_fabric.ipynb` end-to-end to produce
      `Tables/Sales`, `Tables/Products`, `Tables/Customers`
- [ ] Spot-check the tables via the SQL endpoint
- [ ] Verify Step 6's region rollup matches the local pandas run

*(checkboxes get filled in as we complete each step together)*

### Gotcha we hit: generic filenames collide across exercises
Reused a filename (`sales_raw.json`) across two unrelated schemas without
realizing a different exercise already owned that path — the second upload
silently overwrote the first. Lesson for later modules: treat
`Files/raw/<name>.json` as a shared namespace per Lakehouse, not scoped to
whichever notebook you're thinking about at the moment.

---

## Glossary (growing as we go)

| Term | Definition |
|---|---|
| Workspace | Fabric's collaboration/permissions container; backed by one capacity |
| Lakehouse | Fabric item pairing unmanaged `Files/` storage with managed Delta `Tables/` |
| OneLake | Tenant-wide logical data lake; single copy of data, shared across engines |
| Delta table | Parquet files + transaction log; gives ACID updates, time travel, schema enforcement |
| Medallion architecture | Bronze (raw) → Silver (cleaned/conformed) → Gold (curated/aggregated) layering pattern |
