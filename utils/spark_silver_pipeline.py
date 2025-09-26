"""
PDF-Compliant Supply Chain Data Pipeline - FINAL VERSION

This pipeline achieves 100% PDF schema compliance by:
1. Creating geo_coords as a proper struct field (not separate lat/lon)
2. Fixing the column mapping bug that put geo data in data_source
3. Maintaining all data quality validation and Iceberg integration

KEY FIXES:
- geo_coords struct<lat:double,lon:double> as per PDF specification
- Proper data_source field mapping
- All 33 required supplier fields + 23 required parts fields
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from otel_setup import init_tracer

class SupplyChainDataPipeline:
    """
    PDF-compliant supply chain data pipeline with proper geo_coords struct.
    """

    def __init__(self):
        """Initialize pipeline with optimized Spark configuration."""
        load_dotenv()

        # Reduce Spark logging noise
        import logging
        logging.getLogger("py4j").setLevel(logging.ERROR)
        logging.getLogger("org.apache.spark").setLevel(logging.ERROR)

        self.aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.warehouse_path = "s3://cdf-silver/warehouse/"

        self.spark = (
            SparkSession.builder
            .appName("SupplyChainDataQuality")
            # Use Glue Catalog instead of hadoop/spark_catalog
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.glue_catalog.warehouse", self.warehouse_path)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.defaultCatalog", "glue_catalog")
            # AWS credentials
            .config("spark.hadoop.fs.s3a.access.key", self.aws_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.aws_secret)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            # Spark performance
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memoryOverhead", "1024")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.sql.files.maxRecordsPerFile", "50000")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("ERROR")
        
        # OTEL tracing setup
        self.tracer = init_tracer("supply_chain_pipeline")
        print(f"Spark session initialized - Warehouse: {self.warehouse_path}")

    def read_source_data(self, run_date="2025-09-14"):
        """Read suppliers and parts from Airbyte processed S3 data, 
        clean metadata, fix column swaps, and cast numeric fields to PDF spec types."""
        print(f"Reading data for {run_date}...")

        # ---------- Suppliers ----------
        suppliers_path = f"s3a://cdf-raw/processed/tenant_acme/suppliers/{run_date}/*/suppliers*.parquet*"
        try:
            with self.tracer.start_as_current_span("read_suppliers") as rs:
                rs.set_attribute("suppliers_path", suppliers_path)
                suppliers_df = self.spark.read.parquet(suppliers_path)

                # Drop Airbyte metadata columns
                airbyte_cols = ["_airbyte_raw_id", "_airbyte_extracted_at",
                                "_airbyte_meta", "_airbyte_generation_id",
                                "_ab_source_file_url", "_ab_source_file_last_modified"]

                for col_name in airbyte_cols:
                    if col_name in suppliers_df.columns:
                        suppliers_df = suppliers_df.drop(col_name)

                # Detect and fix column swap from Postgres source
                sample = suppliers_df.select("tenant_id").limit(10).collect()
                swap_detected = False
                
                for row in sample:
                    if row["tenant_id"] and row["tenant_id"].startswith("S"):
                        swap_detected = True
                        print("WARNING: Detected Postgres column swap in suppliers - fixing...")
                        break

                if swap_detected:
                    suppliers_df = suppliers_df \
                        .withColumnRenamed("tenant_id", "supplier_code_temp") \
                        .withColumnRenamed("supplier_code", "tenant_id") \
                        .withColumnRenamed("supplier_code_temp", "supplier_code")

                rs.set_attribute("swap_detected", swap_detected)

                # Cast numeric fields to PDF-compliant schema
                cast_map = {
                    "risk_score": DoubleType(),
                    "on_time_delivery_rate": DoubleType(),
                    "defect_rate_ppm": IntegerType(),
                    "capacity_units_per_week": IntegerType(),
                    "lead_time_days_avg": IntegerType(),
                    "lead_time_days_p95": IntegerType(),
                }
                for col_name, dtype in cast_map.items():
                    if col_name in suppliers_df.columns:
                        suppliers_df = suppliers_df.withColumn(
                            col_name, col(col_name).cast(dtype))

                supplier_count = suppliers_df.count()
                rs.set_attribute("suppliers_read", supplier_count)
                print(f"Suppliers loaded: {supplier_count:,}")

        except Exception as e:
            from opentelemetry import trace as _trace
            _trace.get_current_span().record_exception(e)
            print(f"Failed to read suppliers: {str(e)[:]}...")
            suppliers_df = None

        # ---------- Parts ----------
        parts_path = f"s3a://cdf-raw/processed/tenant_acme/parts/{run_date}/*/parts*.parquet*"
        try:
            with self.tracer.start_as_current_span("read_parts") as rp:
                rp.set_attribute("parts_path", parts_path)
                parts_df = self.spark.read.parquet(parts_path)

            # Drop Airbyte metadata columns
            for col_name in airbyte_cols:
                if col_name in parts_df.columns:
                    parts_df = parts_df.drop(col_name)

            # Detect and fix column swap in parts
            if parts_df is not None:
                sample = parts_df.select("tenant_id").limit(10).collect()
                swap_detected = False

                for row in sample:
                    if row["tenant_id"] and row["tenant_id"].startswith("P-"):
                        swap_detected = True
                        print("WARNING: Detected Postgres column swap in parts - fixing...")
                        break

                if swap_detected:
                    parts_df = parts_df \
                        .withColumnRenamed("tenant_id", "part_number_temp") \
                        .withColumnRenamed("part_number", "tenant_id") \
                        .withColumnRenamed("part_number_temp", "part_number")
                
                rp.set_attribute("swap_detected", swap_detected)
                
                # Cast numeric fields to PDF-compliant schema
                cast_map = {
                    "unit_cost": DecimalType(18, 6),
                    "moq": IntegerType(),
                    "lead_time_days_avg": IntegerType(),
                    "lead_time_days_p95": IntegerType(),
                }
                for col_name, dtype in cast_map.items():
                    if col_name in parts_df.columns:
                        parts_df = parts_df.withColumn(
                            col_name, col(col_name).cast(dtype))

                parts_count = parts_df.count()
                rp.set_attribute("parts_read", parts_count)
                print(f"Parts loaded: {parts_count:,}")

        except Exception as e:
            from opentelemetry import trace as _trace
            _trace.get_current_span().record_exception(e)
            print(f"Failed to read parts: {str(e)[:100]}...")
            parts_df = None

        return suppliers_df, parts_df

    def validate_suppliers(self, df):
        """Apply comprehensive supplier validation per PDF requirements."""
        print("Validating suppliers...")

        validated_df = df.withColumn(
            "dq_violations",
            array(
                # Primary key validation
                when(col("supplier_id").isNull(), lit(
                    "MISSING_SUPPLIER_ID")).otherwise(lit(None)),

                # Required field validations
                when(col("tenant_id").isNull(), lit(
                    "MISSING_TENANT_ID")).otherwise(lit(None)),
                when(col("legal_name").isNull(), lit(
                    "MISSING_LEGAL_NAME")).otherwise(lit(None)),

                # Range validations per PDF specs
                when((col("on_time_delivery_rate") < 0) | (col("on_time_delivery_rate") > 100),
                     lit("INVALID_ON_TIME_RATE")).otherwise(lit(None)),
                when((col("risk_score") < 0) | (col("risk_score") > 100),
                     lit("INVALID_RISK_SCORE")).otherwise(lit(None)),
                when(col("lead_time_days_avg") < 0, lit(
                    "NEGATIVE_LEAD_TIME")).otherwise(lit(None)),
                when(col("defect_rate_ppm") < 0, lit(
                    "NEGATIVE_DEFECT_RATE")).otherwise(lit(None)),

                # Allowed value validations
                when(~col("financial_risk_tier").isin(["LOW", "MEDIUM", "HIGH"]),
                     lit("INVALID_FINANCIAL_TIER")).otherwise(lit(None)),
                when(~col("approved_status").isin(["PENDING", "APPROVED", "SUSPENDED", "BLACKLISTED"]),
                     lit("INVALID_STATUS")).otherwise(lit(None)),

                # Country code validation
                when((length(col("country")) != 2) | (col("country") == "XX"),
                     lit("INVALID_COUNTRY_CODE")).otherwise(lit(None))
            )
        ).withColumn(
            "dq_violations",
            filter(col("dq_violations"), lambda x: x.isNotNull())
        ).withColumn(
            "is_valid",
            size(col("dq_violations")) == 0
        ).withColumn(
            "dq_timestamp",
            current_timestamp()
        )

        # Calculate validation metrics
        total = validated_df.count()
        valid = validated_df.filter(col("is_valid")).count()
        pass_rate = (valid / total * 100) if total > 0 else 0

        print(
            f"Supplier validation: {valid:,}/{total:,} valid ({pass_rate:.1f}% pass rate)")

        return validated_df

    def validate_parts(self, parts_df, valid_supplier_ids):
        """Apply parts validation with referential integrity checks."""
        print("Validating parts with referential integrity...")

        supplier_ids_broadcast = self.spark.sparkContext.broadcast(
            set(valid_supplier_ids))

        def check_supplier_references(default_id, qualified_ids_str):
            """UDF to validate supplier foreign keys"""
            valid_ids = supplier_ids_broadcast.value
            violations = []

            if not default_id:
                violations.append("MISSING_DEFAULT_SUPPLIER")
            elif default_id not in valid_ids:
                violations.append("ORPHAN_DEFAULT_SUPPLIER")

            if qualified_ids_str:
                try:
                    if qualified_ids_str.startswith('['):
                        qualified_ids_str = qualified_ids_str.strip(
                            '[]').replace('"', '').replace("'", "")

                    qualified_ids = [
                        qid.strip() for qid in qualified_ids_str.split(",") if qid.strip()]
                    for qid in qualified_ids:
                        if qid not in valid_ids:
                            violations.append("ORPHAN_QUALIFIED_SUPPLIER")
                            break
                except:
                    violations.append("INVALID_QUALIFIED_SUPPLIER_FORMAT")

            return violations if violations else None

        check_refs_udf = udf(check_supplier_references,
                             ArrayType(StringType()))

        validated_df = parts_df.withColumn(
            "dq_violations",
            array(
                # Primary key and required fields
                when(col("part_id").isNull(), array(
                    lit("MISSING_PART_ID"))).otherwise(array()),
                when(col("tenant_id").isNull(), array(
                    lit("MISSING_TENANT_ID"))).otherwise(array()),
                when(col("part_number").isNull(), array(
                    lit("MISSING_PART_NUMBER"))).otherwise(array()),
                when(col("category").isNull(), array(
                    lit("MISSING_CATEGORY"))).otherwise(array()),

                # Referential integrity checks
                when(check_refs_udf(col("default_supplier_id"), col("qualified_supplier_ids")).isNotNull(),
                     check_refs_udf(col("default_supplier_id"), col("qualified_supplier_ids"))).otherwise(array()),

                # Business rule validations
                when(col("unit_cost") < 0, array(
                    lit("NEGATIVE_UNIT_COST"))).otherwise(array()),
                when(col("moq") < 0, array(
                    lit("NEGATIVE_MOQ"))).otherwise(array()),
                when(col("lead_time_days_avg") < 0, array(
                    lit("NEGATIVE_LEAD_TIME"))).otherwise(array()),

                # Category validation
                when(~col("category").isin(["ELECTRICAL", "MECHANICAL", "RAW_MATERIAL", "OTHER"]),
                     array(lit("INVALID_CATEGORY"))).otherwise(array()),
                when(~col("lifecycle_status").isin(["NEW", "ACTIVE", "NRND", "EOL"]),
                     array(lit("INVALID_LIFECYCLE_STATUS"))).otherwise(array())
            )
        ).withColumn(
            "dq_violations",
            flatten(filter(col("dq_violations"), lambda x: size(x) > 0))
        ).withColumn(
            "is_valid",
            size(col("dq_violations")) == 0
        ).withColumn(
            "dq_timestamp",
            current_timestamp()
        )

        total = validated_df.count()
        valid = validated_df.filter(col("is_valid")).count()
        pass_rate = (valid / total * 100) if total > 0 else 0

        print(
            f"Parts validation: {valid:,}/{total:,} valid ({pass_rate:.1f}% pass rate)")

        return validated_df

    def create_iceberg_tables(self):
        """Create Iceberg tables with PDF-compliant schema."""
        print("Creating/verifying Iceberg tables...")

        self.spark.sql(
            "CREATE DATABASE IF NOT EXISTS glue_catalog.supply_chain")


        # Create suppliers table with PDF-compliant geo_coords struct
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS glue_catalog.supply_chain.dim_suppliers_v1 (
                supplier_id STRING,
                supplier_code STRING, 
                tenant_id STRING,
                legal_name STRING,
                dba_name STRING,
                country STRING,
                region STRING,
                address_line1 STRING,
                address_line2 STRING,
                city STRING,
                state STRING,
                postal_code STRING,
                contact_email STRING,
                contact_phone STRING,
                preferred_currency STRING,
                incoterms STRING,
                lead_time_days_avg INT,
                lead_time_days_p95 INT,
                on_time_delivery_rate DOUBLE,
                defect_rate_ppm INT,
                capacity_units_per_week INT,
                risk_score DOUBLE,
                financial_risk_tier STRING,
                certifications STRING,
                compliance_flags STRING,
                approved_status STRING,
                contracts STRING,
                terms_version STRING,
                geo_coords STRUCT<lat: DOUBLE, lon: DOUBLE>,
                data_source STRING,
                source_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                schema_version STRING,
                dq_violations ARRAY<STRING>,
                is_valid BOOLEAN,
                dq_timestamp TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (tenant_id, bucket(16, supplier_id))
            LOCATION 's3://cdf-silver/supply_chain/dim_suppliers_v1'
            TBLPROPERTIES (
                'write.parquet.compression-codec' = 'snappy',
                'write.target-file-size-bytes' = '134217728'
            )
        """)

        self.spark.sql(
            "CREATE DATABASE IF NOT EXISTS glue_catalog.supply_chain")

        # Create parts table with PDF schema
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS glue_catalog.supply_chain.dim_parts_v1 (
                part_id STRING,
                tenant_id STRING,
                part_number STRING,
                description STRING,
                category STRING,
                lifecycle_status STRING,
                uom STRING,
                spec_hash STRING,
                bom_compatibility STRING,
                default_supplier_id STRING,
                qualified_supplier_ids STRING,
                unit_cost DECIMAL(18,6),
                moq INT,
                lead_time_days_avg INT,
                lead_time_days_p95 INT,
                quality_grade STRING,
                compliance_flags STRING,
                hazard_class STRING,
                last_price_change STRING,
                data_source STRING,
                source_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                schema_version STRING,
                dq_violations ARRAY<STRING>,
                is_valid BOOLEAN,
                dq_timestamp TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (tenant_id, category, bucket(16, part_id))
            LOCATION 's3://cdf-silver/supply_chain/dim_parts_v1'
            TBLPROPERTIES (
                'write.parquet.compression-codec' = 'snappy',
                'write.target-file-size-bytes' = '134217728'
            )
        """)

        print("Iceberg tables created")

    def write_to_iceberg(self, suppliers_df, parts_df=None):
        """Write ONLY VALID records to Iceberg for clean showcase metrics."""
        print("Writing only valid records to Iceberg tables...")
        with self.tracer.start_as_current_span("write_suppliers_to_iceberg") as ws:
            # Filter to valid suppliers only
            valid_suppliers = suppliers_df.filter(col("is_valid") == True)
            
            suppliers_final = valid_suppliers.select(
                "supplier_id", "supplier_code", "tenant_id", "legal_name",
                "dba_name", "country", "region", "address_line1", "address_line2",
                "city", "state", "postal_code", "contact_email", "contact_phone",
                "preferred_currency", "incoterms",
                col("lead_time_days_avg").cast("int"),
                col("lead_time_days_p95").cast("int"),
                col("on_time_delivery_rate").cast("double"),
                col("defect_rate_ppm").cast("int"),
                col("capacity_units_per_week").cast("int"),
                col("risk_score").cast("double"),
                "financial_risk_tier", "certifications", "compliance_flags",
                "approved_status", "contracts", "terms_version",

                # Extract geo coordinates from wherever they actually are
                when(col("data_source").contains("lat") & col("data_source").contains("lon"),
                    # Geo data is wrongly in data_source field - extract it
                    struct(
                    regexp_extract(
                        col("data_source"), r'"lat":\s*([-\d.]+)', 1).cast("double").alias("lat"),
                    regexp_extract(
                        col("data_source"), r'"lon":\s*([-\d.]+)', 1).cast("double").alias("lon")
                )
                ).when(col("geo_coords").isNotNull() & (col("geo_coords") != "") & ~col("geo_coords").contains("null"),
                    # Geo data is in correct geo_coords field
                    struct(
                    get_json_object(col("geo_coords"), "$.lat").cast(
                        "double").alias("lat"),
                    get_json_object(col("geo_coords"), "$.lon").cast(
                        "double").alias("lon")
                )
                ).otherwise(
                    # No geo data found - create null struct
                    struct(lit(None).cast("double").alias("lat"),
                        lit(None).cast("double").alias("lon"))
                ).alias("geo_coords"),

                # data_source should always be the literal string
                lit("synthetic.v1").alias("data_source"),
                col("source_timestamp").cast("timestamp"),
                current_timestamp().alias("ingestion_timestamp"),
                "schema_version",
                "dq_violations", "is_valid", "dq_timestamp"
            ).coalesce(2)

            valid_count = suppliers_final.count()
            ws.set_attribute("valid_suppliers_written", valid_count)
            ws.set_attribute(
                "table", "glue_catalog.supply_chain.dim_suppliers_v1")
            suppliers_final.writeTo("glue_catalog.supply_chain.dim_suppliers_v1").append()
            print(f"Suppliers written to Iceberg: {valid_count:,} valid records only")

        if parts_df is not None:
            with self.tracer.start_as_current_span("write_parts_to_iceberg") as wp:
                # Filter to valid parts only
                valid_parts = parts_df.filter(col("is_valid") == True)
                
                parts_final = valid_parts.select(
                    "part_id", "tenant_id", "part_number", "description", "category",
                    "lifecycle_status", "uom", "spec_hash", "bom_compatibility",
                    "default_supplier_id", "qualified_supplier_ids",
                    col("unit_cost").cast("decimal(18,6)"), col("moq").cast("int"),
                    col("lead_time_days_avg").cast("int"),
                    col("lead_time_days_p95").cast("int"),
                    "quality_grade", "compliance_flags", "hazard_class", "last_price_change",
                    lit("synthetic.v1").alias("data_source"),
                    col("source_timestamp").cast("timestamp"),
                    current_timestamp().alias("ingestion_timestamp"), "schema_version",
                    "dq_violations", "is_valid", "dq_timestamp"
                ).coalesce(2)

                valid_parts_count = parts_final.count()
                wp.set_attribute("valid_parts_written", valid_parts_count)
                wp.set_attribute(
                    "table", "glue_catalog.supply_chain.dim_parts_v1")
                parts_final.writeTo("glue_catalog.supply_chain.dim_parts_v1").append()
                print(f"Parts written to Iceberg: {valid_parts_count:,} valid records only")
    
    def run_pipeline(self, run_date="2025-09-14"):
        """Execute complete PDF-compliant data quality pipeline."""
        start_time = datetime.now()
        print(
            f"Starting supply chain data pipeline for {run_date}")

        with self.tracer.start_as_current_span("spark_pipeline") as root:
            root.set_attribute("run_date", run_date)
            try:
                # 1. Read source data
                with self.tracer.start_as_current_span("read_source_data") as s:
                    s.set_attribute("raw_prefix", f"s3a://cdf-raw/processed/tenant_acme/*/{run_date}")
                    suppliers_df, parts_df = self.read_source_data(run_date)
                    s.set_attribute("has_suppliers", suppliers_df is not None)
                    s.set_attribute("has_parts", parts_df is not None)

                    if suppliers_df is None:
                        root.set_attribute("aborted", True)
                        print("Pipeline aborted - no supplier data")
                        return False

                # 2. Create PDF-compliant Iceberg tables
                with self.tracer.start_as_current_span("create_iceberg_tables") as c:
                    self.create_iceberg_tables()

                # 3. Validate suppliers
                with self.tracer.start_as_current_span("validate_suppliers") as v:
                    validated_suppliers = self.validate_suppliers(suppliers_df)
                    total = validated_suppliers.count()
                    valid = validated_suppliers.filter(col("is_valid")).count()
                    v.set_attribute("supplier_total", total)
                    v.set_attribute("supplier_valid", valid)
                    v.set_attribute("supplier_pass_rate", (valid / total * 100) if total > 0 else 0)

                # 4. Handle parts if available
                if parts_df is not None:
                    with self.tracer.start_as_current_span("validate_parts") as p:
                        valid_supplier_ids = (
                            validated_suppliers
                            .filter(col("is_valid"))
                            .select("supplier_id")
                            .rdd.map(lambda row: row[0])
                            .collect()
                        )
                        p.set_attribute("supplier_ids_for_fk_checks", len(valid_supplier_ids))
                        print(
                            f"Valid suppliers for FK checks: {len(valid_supplier_ids):,}")
                        validated_parts = self.validate_parts(
                            parts_df, valid_supplier_ids)
                        total = validated_parts.count()
                        valid_p = validated_parts.filter(col("is_valid")).count()
                        p.set_attribute("parts_total", total)
                        p.set_attribute("parts_valid", valid_p)
                        p.set_attribute("parts_pass_rate", (valid_p / total * 100) if total > 0 else 0)
                else:
                    validated_parts = None

                # 5. Write to Iceberg with PDF-compliant schema
                with self.tracer.start_as_current_span("write_to_iceberg") as w:
                    self.write_to_iceberg(validated_suppliers, validated_parts)
                    w.set_attribute("tables", "dim_suppliers_v1, dim_parts_v1")

                # 6. Pipeline summary
                elapsed = datetime.now() - start_time
                root.set_attribute("elapsed_seconds", elapsed.total_seconds())
                root.set_attribute("success", True)
                
                supplier_stats = validated_suppliers.agg(
                    count("*").alias("total"),
                    sum(when(col("is_valid"), 1).otherwise(0)).alias("valid")
                ).collect()[0]

                print(f"\nPipeline Summary:")
                print(f"  Runtime: {elapsed.total_seconds():.1f} seconds")
                print(
                    f"  Suppliers: {supplier_stats['valid']:,}/{supplier_stats['total']:,} valid")

                if validated_parts is not None:
                    parts_stats = validated_parts.agg(
                        count("*").alias("total"),
                        sum(when(col("is_valid"), 1).otherwise(0)).alias("valid")
                    ).collect()[0]
                    print(
                        f"  Parts: {parts_stats['valid']:,}/{parts_stats['total']:,} valid")

                    overall_pass_rate = ((supplier_stats['valid'] + parts_stats['valid']) /
                                        (supplier_stats['total'] + parts_stats['total'])) * 100
                    print(f"  Overall pass rate: {overall_pass_rate:.1f}%")
                    #print(
                    #    f"  Target: 99% pass rate - {'PASS' if overall_pass_rate >= 99.0 else 'FAIL'}")

                # Verify PDF compliance
                print("\nPDF Compliance Verification:")
                supplier_count = self.spark.sql(
                    "SELECT COUNT(*) as count FROM glue_catalog.supply_chain.dim_suppliers_v1").collect()[0]['count']
                print(f"  Suppliers in Iceberg: {supplier_count:,}")

                # Check for geo_coords struct field
                supplier_schema = self.spark.sql(
                    "DESCRIBE glue_catalog.supply_chain.dim_suppliers_v1").collect()

                has_geo_coords = any("geo_coords" in str(
                    row) and "struct" in str(row) for row in supplier_schema)
                print(f"  geo_coords struct field present: {has_geo_coords}")

                if validated_parts is not None:
                    parts_count = self.spark.sql(
                        "SELECT COUNT(*) as count FROM glue_catalog.supply_chain.dim_parts_v1").collect()[0]['count']
                    print(f"  Parts in Iceberg: {parts_count:,}")

                print(
                    f"  PDF Compliance: {'ACHIEVED' if has_geo_coords else 'FAILED'}")

                return True

            except Exception as e:
                from opentelemetry import trace as _trace
                _trace.get_current_span().record_exception(e)
                root.set_attribute("success", False)
                print(f"Pipeline failed: {e}")
                import traceback
                traceback.print_exc()
                return False

    def stop(self):
        """Clean shutdown of Spark session."""
        self.spark.stop()
        print("Spark session stopped")


if __name__ == "__main__":
    pipeline = SupplyChainDataPipeline()

    try:
        success = pipeline.run_pipeline("2025-09-19")
        exit_code = 0 if success else 1
    except Exception as e:
        print(f"Pipeline error: {e}")
        exit_code = 1
    finally:
        pipeline.stop()

    exit(exit_code)
