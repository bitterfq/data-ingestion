# Data Ingestion Pipeline PoC

## Overview
Enterprise-grade, multi-tenant supply chain data pipeline for ingesting, validating, and curating supplier and part data at scale. Supports both S3 and PostgreSQL as customer landing zones, with robust data quality validation, referential integrity, and modern lakehouse architecture (Iceberg + AWS Glue). Designed for batch and incremental workflows.

---

## Project Structure

```
data-ingestion/
├── data_generator/
│   ├── generator.py
│   ├── alt_generator.py
│   ├── __init__.py
│   └── data/                # Generated CSV/Parquet files
│
├── utils/
│   ├── spark_silver_pipeline.py
│   ├── otel_setup.py
│   ├── s3_list.py
│   └── clean_infra.sh
│
├── tests/
│   ├── test_generator.py
│   └── pdf_compliance_test.py
│
├── docs/
│   ├── batch-pipeline-architecture.md
│   ├── data_generator.md
│   └── learnings.md
│
├── docker/
│   └── Dockerfile.generator
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

- **Data Generation**: Python (vectorized, high-performance, dirty data injection)
- **Ingestion**: Airbyte (CSV/Parquet/PG connectors)
- **Landing Zone**: S3 or PostgreSQL (customer-provided)
- **Raw Zone**: S3 (`cdf-raw` bucket)
- **Processing**: Apache Spark (validation, transformation, referential integrity)
- **Silver Zone**: Apache Iceberg tables in S3 (`cdf-silver` bucket)
- **Catalog**: AWS Glue (Iceberg metadata, LlamaIndex integration)
- **Quality**: Built-in validation, referential integrity, anomaly injection, PDF-compliant schema

---

## Data Contracts

- **Suppliers**: 33 fields (risk metrics, certifications, geo-coords as struct, compliance flags, etc.)
- **Parts**: 23 fields (qualified suppliers, costs, lead times, FKs, etc.)
- **Validation**: PK uniqueness, FK integrity, business rules, freshness checks
- **Schema**: PDF-compliant with `geo_coords` as struct<lat:double,lon:double>

---

## Key Features

- Multi-tenant isolation (tenant_id partitioning)
- Deterministic, realistic data generation with dirty data injection
- Automatic schema evolution and drift handling
- Quarantine pattern for data quality failures
- Complete audit trail and lineage tracking
- Idempotent processing with safe reruns
- UPSERT pattern for PostgreSQL operations
- OpenTelemetry tracing for pipeline observability
- High-performance generator (100k+ records in <2 minutes)

---

## Performance Targets

- **Baseline**: 60k records in <15 minutes (t3.medium equivalent)
- **Scale**: 600k records with linear scaling (10x records = 2x resources)
- **Data Quality**: 99% validation pass rate
- **Concurrency**: 10+ parallel ingestion jobs supported

---

## Getting Started

### Prerequisites
- Python 3.8+
- AWS CLI & credentials configured
- PostgreSQL (optional, for local testing)
- Apache Spark 3.4+ with Iceberg support
- Airbyte (for ingestion)

### Quick Start

1. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

2. **Set up environment variables**
    ```bash
    cp .env.example .env
    # Edit .env with your AWS and PG credentials
    ```

3. **Generate test data**
    ```bash
    cd data_generator
    python generator.py
    # Or for high-performance: python alt_generator.py --num_suppliers 100000 --num_parts 100000
    ```

4. **Run validation pipeline**
    ```bash
    python utils/spark_silver_pipeline.py
    ```

5. **(Optional) Local PostgreSQL testing**
    ```bash
    docker-compose up -d postgres
    python data_generator/generator.py
    ```

6. **Clean up infrastructure**
    ```bash
    bash utils/clean_infra.sh
    ```

---

## Known Issues

- **Double Write Bug**: Records accumulate instead of replace on subsequent runs
- **S3 timestamp drift**: Files split across folders due to ingestion time
- **Single-tenant paths**: Hardcoded paths limit multi-tenant deployments
- **Airbyte schema drift**: Binary data type errors on reruns

See GitHub Issues for detailed reproduction steps and proposed fixes.

---

## Configuration

### Environment Variables
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1

# PostgreSQL (Optional)
POSTGRES_HOST=localhost
POSTGRES_USER=pipeline_user
POSTGRES_PASSWORD=pipeline_pass
POSTGRES_DB=supply_chain
```

### S3 Buckets
- `cdf-upload`: Customer file uploads (CSV/Parquet)
- `cdf-raw`: Airbyte processed files (Parquet)
- `cdf-silver`: Iceberg tables (curated data)

---

## Documentation

- [docs/batch-pipeline-architecture.md](docs/batch-pipeline-architecture.md) - Detailed architecture
- [docs/data_generator.md](docs/data_generator.md) - Data generation specs
- [docs/learnings.md](docs/learnings.md) - Implementation insights

---

## Testing

- Unit tests: `pytest tests/`
- PDF compliance: `python tests/pdf_compliance_test.py`
- Generator tests: `python tests/test_generator.py`
