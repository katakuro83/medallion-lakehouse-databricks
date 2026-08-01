# Medallion Lakehouse Architecture on Databricks

Bronze/Silver/Gold lakehouse on **Databricks** with schema enforcement, data quality checks, and incremental (CDC) processing.

**Stack:** Databricks · Delta Lake · PySpark

---

## Architecture

```
┌────────────────┐    ┌───────────────────┐    ┌────────────────────┐    ┌────────────────────┐
│  CDC event feed │ -> │   BRONZE (raw)     │ -> │   SILVER (clean)    │ -> │   GOLD (curated)     │
│  (insert/update/│    │  Auto Loader        │    │  MERGE INTO (CDC     │    │  Business aggregates │
│   delete)       │    │  schema enforced    │    │  apply changes),     │    │  ready for BI         │
│                  │    │  + rescued data col │    │  DQ checks +         │    │                       │
│                  │    │                     │    │  quarantine          │    │                       │
└────────────────┘    └───────────────────┘    └────────────────────┘    └────────────────────┘
```

**Layers:**

| Layer | Purpose | Format | Processing |
|---|---|---|---|
| **Bronze** | Raw landing zone: data exactly as received, append-only, full history | Delta | Auto Loader (`cloudFiles`), explicit schema + `rescuedDataColumn` for drift |
| **Silver** | Deduplicated, validated, current-state tables | Delta | `MERGE INTO` applying CDC operations (insert/update/delete) by sequence number |
| **Gold** | Aggregated, business level tables for dashboards/reporting | Delta | Incremental aggregation, `OPTIMIZE` + `ZORDER` |

See Architecture Deep Dive for the full design rationale, including how CDC apply changes and data quality quarantine work.

---

## What Makes This "Production Shaped"

- **Schema enforcement, not schema on read** Bronze tables have an explicit `StructType`; anything that doesn't conform lands in a `_rescued_data` column instead of silently corrupting the table or failing the whole batch.
- **CDC apply changes, not blind appends** Silver uses `MERGE INTO` keyed on a sequence/timestamp column, so out of order events don't overwrite newer data, and deletes are honored, not ignored.
- **Data quality as a pipeline stage, not an afterthought** every Silver write runs through `src/utils/data_quality.py`, which checks null rates, uniqueness, and referential integrity, and routes failing rows to a quarantine table instead of the mainline.
- **Incremental Gold** Gold aggregates only reprocess affected partitions rather than recomputing the full history on every run.

---

## Project Structure

```
medallion-lakehouse/
├── data/raw/orders_cdc/        # Sample CDC batches (insert/update/delete events)
├── src/
│   ├── bronze/
│   │   └── ingest_bronze.py    # Auto Loader ingestion, schema enforcement
│   ├── silver/
│   │   └── apply_cdc_silver.py # MERGE INTO CDC apply + DQ + quarantine
│   ├── gold/
│   │   └── build_gold_aggregates.py  # Incremental business aggregates
│   └── utils/
│       ├── schemas.py          # Explicit StructType schema definitions
│       └── data_quality.py     # Reusable DQ check framework
├── tests/
│   └── test_data_quality.py    # Local pytest suite (runs without Databricks)
├── jobs/
│   └── databricks_job.yml      # Databricks Workflow/Asset Bundle job definition
├── architecture/architecture.md
└── docs/setup_guide.md
```

---

## Possible Extensions

- Replace file based CDC batches with a live feed (Debezium → Kafka → Auto Loader)
- Add **Delta Live Tables (DLT)** expectations as an alternative to the custom DQ module
- Add Unity Catalog table constraints (`NOT NULL`, `CHECK`) at the Silver/Gold layer
- Add a `_dq_metrics` Delta table so data quality trends are queryable over time, not just pass/fail per run

---

## License

MIT
