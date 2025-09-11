# Complete Multi-Tenant Supply Chain Data Pipeline Project Context

## Technical Requirements (from PDF)
- **Scale**: 30,000 suppliers + 30,000 parts per tenant
- **SLA**: 15-minute end-to-end processing 
- **Data Quality**: 99% pass rate with comprehensive validation
- **Multi-tenant isolation**: Partitioned by tenant_id
- **Referential integrity**: Parts must reference valid suppliers
- **Storage**: Apache Iceberg tables in S3 for analytics

## Architecture Evolution
**Started with**: MinIO-based local storage → **Migrated to**: AWS S3 for production readiness due to Airbyte connectivity bugs with MinIO → **Dropped Kafka complexity** entirely due to operational overhead and batch-oriented workload mismatch → **Simplified to direct batch processing**: S3 upload → Airbyte → S3 raw → Spark → S3 Iceberg warehouse

## Current Technical Stack
- **Storage**: AWS S3 (3 buckets: cdf-upload, cdf-raw, cdf-silver)
- **Processing**: Apache Spark with Iceberg using AWS Glue catalog
- **Ingestion**: Airbyte (CSV to Parquet transformation)
- **Validation**: Comprehensive data quality rules per PDF schema
- **Infrastructure**: Docker Compose locally, S3 for storage

## Data Schema Implementation
**Suppliers Table (dim_suppliers_v1)**:
- supplier_id (PK), tenant_id, legal_name, country, risk_score (0-100)
- on_time_delivery_rate (0-100%), financial_risk_tier (LOW/MEDIUM/HIGH)
- lead_time_days_avg, defect_rate_ppm, certifications, approved_status
- Partitioned by tenant_id + bucket(16, supplier_id)

**Parts Table (dim_parts_v1)**:
- part_id (PK), tenant_id, part_number, category, lifecycle_status
- default_supplier_id (FK), qualified_supplier_ids (array), unit_cost, moq
- Referential integrity: all supplier FKs must exist in valid suppliers
- Partitioned by tenant_id + category + bucket(16, part_id)

## Major Issues Resolved
1. **MinIO Connectivity Issues**: Airbyte had persistent connection bugs with MinIO requiring custom networking configurations. Migration to AWS S3 resolved connectivity issues and provided native Airbyte connector support.

2. **Airbyte Schema Inconsistency**: Parts connector was using raw JSON normalization (data in NULL `data` column) while suppliers used flattened structure. Fixed by ensuring both use same normalization approach.

3. **Data Generator Schema Stability**: Fixed parts CSV headers with deterministic column ordering to prevent Airbyte parsing issues.

4. **Spark Array Type Mismatch**: Fixed validation logic mixing STRING and ARRAY<STRING> types in parts validation.

## Current Working Pipeline Status
**End-to-End Processing**: 
- Generator → S3 Upload → Airbyte → S3 Raw → Spark → Iceberg warehouse
- Both suppliers and parts processing with full referential integrity validation
- Runtime: ~1m55s for 100k suppliers, ~1m9s for 100k parts (well under 15-minute target)

**Data Quality Framework**:
- Suppliers: 97.5% pass rate (comprehensive validation working)
- Parts: Variable pass rate depending on referential integrity (45-93%)
- FK violation detection: Parts → Valid suppliers enforced

**S3 Iceberg Implementation**:
- Tables created with proper schema per PDF specifications
- Multi-tenant partitioning working correctly
- Type casting and data writing successful
- Append-mode operations accumulating data correctly

## Current Scale Testing Results
- **100k suppliers**: 1m55s processing time
- **100k parts**: 1m9s processing time
- Processing rate: ~870-1500 records/second through entire pipeline
- Performance well under 15-minute SLA target

## Architecture Decisions Made
- **AWS Glue catalog integration** for Iceberg metadata management
- **Direct type casting** handles Airbyte string columns properly  
- **Comprehensive error handling** with detailed violation reporting
- **Multi-tenant partitioning** with bucket hashing for distribution
- **Batch processing focus**: Dropped Kafka due to operational complexity mismatch with batch-oriented CSV file workloads and 15-minute SLA requirements

## Immediate Optimizations

**Airbyte Incremental Processing**: Currently Airbyte rewrites entire destination buckets on each sync, causing inefficiencies at scale. Immediate implementation needed:
- Configure incremental sync modes to append only new/changed records
- Implement partition-aware writes to date-partitioned S3 paths
- Use `source_timestamp` as cursor field for change detection
- Expected impact: 80% reduction in processing time for incremental updates