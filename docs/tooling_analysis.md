# Data Pipeline Tooling Analysis

## Ingestion Layer Comparison

| Tool | Throughput | Setup Complexity | Enterprise Features | Cost at Scale |
|------|------------|------------------|-------------------|---------------|
| **Airbyte** | Medium | Low | High (300+ connectors, CDC, schema drift) | Fixed per connector |
| **Debezium + Kafka** | High | High | High (streaming, exactly-once) | Linear with volume |
| **Fivetran** | Medium | Very Low | High (managed, compliance) | Expensive ($1-3M rows) |

**Analysis**: For the PoC requirements of 30k suppliers + 30k parts, Airbyte provides the best balance. The built-in connectors demonstrate enterprise integration capabilities while the UI simplifies stakeholder demos. Debezium requires Kafka cluster management that adds operational overhead without clear benefit at this scale. Fivetran pricing makes the scaling story unrealistic.

**Performance Impact**: At 60k records, ingestion layer choice minimally affects end-to-end timing. Focus is on connector reliability and demo credibility.

## Storage Layer Comparison

| Format | Query Speed | Multi-Engine | Schema Evolution | Operational Overhead |
|--------|-------------|--------------|------------------|----------------------|
| **Iceberg** | Excellent | Best | Automatic | Medium |
| **Delta Lake** | Good | Spark-focused | Good | Low |
| **Hudi** | Good | Limited | Complex | High |

**Analysis**: Iceberg's hidden partitioning eliminates partition key management complexity. Time travel capabilities and ACID guarantees meet enterprise reliability requirements. The multi-engine support (Spark, Trino, Dremio) demonstrates vendor independence.

**Performance Impact**: Partition pruning reduces scan costs by 70-90% as data scales. Metadata optimization keeps query times flat regardless of table size growth.

## Processing Layer Comparison

| Tool | Data Size Sweet Spot | Memory Requirements | Learning Curve | Enterprise Adoption |
|------|---------------------|---------------------|----------------|-------------------|
| **Spark** | 1GB+ | 2-3x data size | Medium | High |
| **DuckDB** | <1GB | 1x data size | Low | Growing |
| **Flink** | Streaming workloads | 1.5x data size | High | Medium |

**Analysis**: Given the enterprise-ready requirement, Spark is the appropriate choice. The 60k record dataset fits well within Spark's capabilities and provides clear scaling demonstration to 600k+ records. DuckDB would be faster for the PoC but creates a migration requirement for production scale.

**Performance Impact**: Expected processing time for 60k records with complex transformations (deduplication, address normalization, risk scoring): 8-12 minutes on 4-node cluster.

## Orchestration Layer Comparison

| Tool | Setup Time | Python Integration | Enterprise Features | Learning Curve |
|------|------------|-------------------|-------------------|----------------|
| **Airflow** | High | Native | Mature (monitoring, alerting) | Medium |
| **Prefect** | Medium | Native | Modern (better UX) | Low |
| **Dagster** | Medium | Native | Asset-focused | High |

**Analysis**: Airflow provides the most enterprise-recognizable solution with comprehensive operator ecosystem. The Python-native approach supports custom business logic for supplier data transformations.

## Performance Requirements Analysis

### Baseline Target (30k + 30k records)
- **Data Volume**: ~150MB total (suppliers: ~75MB, parts: ~75MB)
- **Processing Time**: 15-20 minutes end-to-end including validation
- **Resource Requirements**: 4-node Spark cluster (16 cores, 32GB RAM total)

### Scale Target (300k + 300k records)  
- **Data Volume**: ~1.5GB total
- **Processing Time**: 45-60 minutes end-to-end
- **Resource Requirements**: 12-node Spark cluster (48 cores, 96GB RAM total)
- **Cost Scaling**: 3x cluster size for 10x data volume due to coordination overhead

### Validation Performance
- **Great Expectations Suite**: <2 minutes for complete validation
- **Data Quality Gates**: 99% pass rate target with <1% quarantine
- **FK Validation**: All qualified_supplier_ids must resolve to valid supplier records

## Final Architecture Proposal

### Core Stack
- **Ingestion**: Airbyte for connector ecosystem and demo credibility
- **Storage**: Iceberg for enterprise features and scaling story  
- **Processing**: Spark for production readiness and transformation complexity
- **Orchestration**: Airflow for enterprise familiarity and Python flexibility

### Supporting Infrastructure
- **Data Quality**: Great Expectations with quarantine pattern
- **Monitoring**: Prometheus + Grafana for pipeline observability
- **Lineage**: OpenLineage for data provenance tracking

### Partitioning Strategy
- **Suppliers**: `tenant_id, days(ingestion_timestamp)`
- **Parts**: `tenant_id, category, days(ingestion_timestamp)`

This approach optimizes for common query patterns while maintaining manageable partition counts.

## Implementation Approach

### Phase 1: Core Pipeline (Week 1-2)
- Airbyte connector configuration for synthetic data sources
- Iceberg table setup with defined schemas and partitioning
- Spark transformation jobs for data standardization and quality checks
- Basic Airflow DAGs for pipeline orchestration

### Phase 2: Validation & Monitoring (Week 3-4)  
- Great Expectations suite implementation
- Data quality quarantine and remediation workflows
- Prometheus metrics collection and Grafana dashboards
- End-to-end testing with 60k record dataset

### Phase 3: Scale Demonstration (Week 5-6)
- Performance testing with 600k record dataset
- Resource optimization and cost analysis
- Advanced monitoring and alerting configuration
- Documentation and operational procedures
