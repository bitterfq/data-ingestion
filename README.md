# Data Ingestion Pipeline PoC

## Overview
Enterprise-ready, multi-tenant data pipeline for ingesting and validating supplier and part data at scale. Supports both S3 and PostgreSQL as customer landing zones, with robust data quality validation, referential integrity, and modern lakehouse architecture.

---

## Project Structure

```
data-ingestion/
├── data_generator/
│   ├── generator.py
│   └── data/                # Generated CSV/Parquet files
│
├── bridge_service/
│   └── bridge_service.py
│
├── utils/
│   ├── spark_silver_pipeline.py
│   └── s3_list.py
│
├── docs/
│   ├── batch-pipeline-architecture.md
│   ├── data_generator.md
│   └── learnings.md
│
├── requirements.txt
├── docker-compose.yml
├── .env                     # Environment variables (not committed)
└── README.md
```

---

## Architecture Overview

### Supported Ingestion Paths

1. **S3 Landing Zone**
    - Generator (CSV, Parquet) → Customer S3 Upload → Airbyte → S3 Raw → Spark → Silver (Iceberg) → AWS Glue (LlamaIndex)

2. **PostgreSQL Landing Zone**
    - Generator (CSV, Parquet) → PostgreSQL (Customer) → Airbyte → S3 Raw → Spark → Silver (Iceberg) → AWS Glue (LlamaIndex)

### Core Stack

- **Ingestion**: Airbyte (CSV/Parquet/PG connectors)
- **Landing Zone**: S3 or PostgreSQL (customer-provided)
- **Raw Zone**: S3 (`cdf-raw` bucket)
- **Processing**: Apache Spark (validation, transformation)
- **Silver Zone**: Apache Iceberg tables in S3 (`cdf-silver` bucket)
- **Catalog**: AWS Glue (Iceberg metadata, LlamaIndex integration)
- **Orchestration**: (Optional) Apache Airflow
- **Quality**: Built-in validation, referential integrity, and anomaly injection

---

## Data Contracts

- **Suppliers**: 23+ fields (risk metrics, certifications, geo-coords, compliance flags)
- **Parts**: 20+ fields (qualified suppliers, costs, lead times, FKs)
- **Validation**: PK uniqueness, FK integrity, business rules, freshness checks

---

## Key Features

- Multi-tenant isolation (tenant_id partitioning)
- Deterministic, realistic data generation with dirty data injection
- Automatic schema evolution and drift handling
- Quarantine pattern for data quality failures
- Complete audit trail and lineage tracking
- Idempotent processing with safe reruns

---

## Performance Targets

- **Baseline**: 60k records in <15 minutes (4-node Spark cluster)
- **Scale**: 600k records in <60 minutes (12-node Spark cluster)
- **Data Quality**: 99% validation pass rate

---

## Getting Started

1. **Install dependencies**
    ```sh
    pip install -r requirements.txt
    ```

2. **Set up environment variables**
    - Copy `.env.example` to `.env` and fill in AWS/PG credentials.

3. **Generate data**
    ```sh
    python data_generator/generator.py
    ```

4. **Run pipeline**
    ```sh
        utils/spark_silver_pipeline.py
    ```

5. **(Optional) Use Docker Compose for local stack**
    ```sh
    docker-compose up
    ```

---

## Documentation

- [docs/batch-pipeline-architecture.md](docs/batch-pipeline-architecture.md)
- [docs/data_generator.md](docs/data_generator.md)
-