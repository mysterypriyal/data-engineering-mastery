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
- **Silver**: validated, typed, deduplicated — `Tables/silver_sales`,
  produced by `clean_sales_data_fabric.ipynb`.
- **Gold** (not yet built): curated, business-level aggregates for reporting
  — to be added once silver is in place and validated.

### Exercise 1 status
- [ ] Create the **Fabric Learning** workspace
- [ ] Create a Lakehouse inside it
- [ ] Upload `sales_raw.json`, `customers.json`, `products.json` to
      `Files/raw/`
- [ ] Attach `clean_sales_data_fabric.ipynb` to the Lakehouse and run it
      end-to-end to produce `Tables/silver_sales`
- [ ] Spot-check `silver_sales` via the SQL endpoint

*(checkboxes get filled in as we complete each step together)*

---

## Glossary (growing as we go)

| Term | Definition |
|---|---|
| Workspace | Fabric's collaboration/permissions container; backed by one capacity |
| Lakehouse | Fabric item pairing unmanaged `Files/` storage with managed Delta `Tables/` |
| OneLake | Tenant-wide logical data lake; single copy of data, shared across engines |
| Delta table | Parquet files + transaction log; gives ACID updates, time travel, schema enforcement |
| Medallion architecture | Bronze (raw) → Silver (cleaned/conformed) → Gold (curated/aggregated) layering pattern |
