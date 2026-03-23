# ETL Pipeline with Medallion Architecture — Cat Image API

A hands-on data engineering project built to practise **end-to-end ETL pipeline development** using industry-standard tools and the **Medallion Architecture** (Raw → Stage → Trusted → Delivery) on Databricks.

---

## Overview

This project simulates a real-world data ingestion and transformation workflow using a public REST API as the data source. The goal was to apply enterprise data engineering patterns — the same patterns used in production environments — in a low-risk, reproducible setting.

Data is extracted from [The Cat API](https://thecatapi.com/), transformed progressively through structured layers, and stored using **Delta Lake** for reliable, versioned data management.

---

## Architecture

```
REST API (The Cat API)
        │
        ▼
┌─────────────┐
│   RAW Layer │  ← Raw JSON responses ingested as-is from the API
└──────┬──────┘
       │
       ▼
┌──────────────┐
│  STAGE Layer │  ← Data parsed into Spark DataFrames, schema enforced
└──────┬───────┘
       │
       ▼
┌────────────────┐
│ TRUSTED Layer  │  ← Data cleaned, deduplicated, and validated
└──────┬─────────┘
       │
       ▼
┌──────────────────┐
│ DELIVERY Layer   │  ← Final curated dataset ready for consumption
└──────────────────┘
```

Each layer is stored as a **Delta Lake table**, enabling time travel, schema enforcement, and ACID transactions.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Databricks** | Unified analytics platform and notebook environment |
| **Apache Spark / PySpark** | Distributed data processing |
| **Delta Lake** | Storage layer with ACID transactions and versioning |
| **Python** | API extraction and data transformation logic |
| **REST API (JSON)** | External data source |

---

## Project Structure

```
ETL_CAT_API/
│
├── NboRawApiTest.ipynb       # Layer 1: API extraction → Raw Delta table
├── NboStageApiTest.ipynb     # Layer 2: Schema enforcement → Stage Delta table
├── NboTrustedApiTest.ipynb   # Layer 3: Data cleaning → Trusted Delta table
├── NboDeliveryApiTest.ipynb  # Layer 4: Final dataset → Delivery Delta table
└── README.md
```

---

## What I Practised

- Consuming a public REST API and ingesting JSON responses into Spark DataFrames
- Implementing the **Medallion Architecture** pattern used in enterprise data lakes
- Working with **Delta Lake** for versioned, reliable storage
- Applying schema enforcement and data validation between pipeline layers
- Using **Databricks notebooks** as an orchestration and development environment

---

## How to Run

1. Clone this repository
2. Import the notebooks into your **Databricks workspace** (File → Import)
3. Run each notebook in order: Raw → Stage → Trusted → Delivery
4. A free Databricks Community Edition account is sufficient to run this project

> **Note:** No API key is required for basic usage of The Cat API.

---

## Author

**Pedro Ribeiro** — Data Analyst & BI Developer based in Sydney, Australia  
[LinkedIn](https://www.linkedin.com/in/pedroribeiroit/) · [GitHub](https://github.com/Mousinho)
