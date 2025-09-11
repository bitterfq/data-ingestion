"""
Complete Supply Chain Data Quality Pipeline with Iceberg

This module defines a pipeline for validating and processing supplier and part data
from S3, applying comprehensive data quality checks and referential integrity validation,
and writing the results to Apache Iceberg tables in S3.

Features:
- Reads from Airbyte-processed S3 data (handles both flattened and raw formats)
- Comprehensive data quality validation per requirements
- Referential integrity checking (parts -> suppliers)
- Writes to Apache Iceberg tables in S3 with proper partitioning
- Production-ready error handling and metrics
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime


class SupplyChainDataPipeline:
    """
    Pipeline for supply chain data quality validation and Iceberg ingestion.

    Methods:
        read_source_data(run_date): Load suppliers and parts from S3.
        validate_suppliers(df): Apply data quality checks to suppliers.
        validate_parts(parts_df, valid_supplier_ids): Validate parts and check referential integrity.
        create_iceberg_tables(): Create Iceberg tables if not present.
        write_to_iceberg(suppliers_df, parts_df): Write validated data to Iceberg.
        run_pipeline(run_date): Execute the full pipeline.
        stop(): Clean up Spark session.
    """

    def __init__(self):
        """
        Initialize pipeline with S3 Iceberg configuration and minimal logging.
        Loads AWS credentials from environment variables.
        """
        load_dotenv()

        # Reduce Spark logging noise
        import logging
        logging.getLogger("py4j").setLevel(logging.ERROR)
        logging.getLogger("org.apache.spark").setLevel(logging.ERROR)

        self.aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.warehouse_path = "s3a://cdf-silver/warehouse/"

        # Create Spark session with minimal logging
        self.spark = (
            SparkSession.builder
            .appName("SupplyChainDataQuality")
            .config("spark.sql.catalog.cdf", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.cdf.type", "hadoop")
            .config("spark.sql.catalog.cdf.warehouse", self.warehouse_path)
            .config("spark.hadoop.fs.s3a.access.key", self.aws_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.aws_secret)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        # Set log level to reduce noise
        self.spark.sparkContext.setLogLevel("ERROR")
        print(f"Spark session initialized - Warehouse: {self.warehouse_path}")

    def read_source_data(self, run_date="2025-09-10"):
        """
        Read suppliers and parts from Airbyte processed S3 data.

        Args:
            run_date (str): Date string for partitioned S3 data.

        Returns:
            tuple: (suppliers_df, parts_df)
        """
        print(f"Reading data for {run_date}...")

        # Read suppliers (all parquet files including suffixed ones)
        suppliers_path = f"s3a://cdf-raw/processed/tenant_acme/suppliers/{run_date}/*/suppliers*.parquet*"
        try:
            suppliers_df = self.spark.read.parquet(suppliers_path)
            supplier_count = suppliers_df.count()
            print(f"Suppliers loaded: {supplier_count:,}")
        except Exception as e:
            print(f"Failed to read suppliers: {str(e)[:100]}...")
            suppliers_df = None

        # Read parts (all parquet files including suffixed ones)
        parts_path = f"s3a://cdf-raw/processed/tenant_acme/parts/{run_date}/*/parts*.parquet*"
        try:
            parts_df = self.spark.read.parquet(parts_path)

            # Check if parts are still in broken format (data column) or fixed (flattened)
            if "data" in parts_df.columns and "part_id" not in parts_df.columns:
                print(
                    "Parts still in raw format - checking if data column has content now...")
                non_null_data = parts_df.filter(
                    col("data").isNotNull()).count()
                if non_null_data > 0:
                    print(
                        f"Found {non_null_data} non-null data records - parsing JSON...")
                    # Parse JSON data column
                    parts_schema = StructType([
                        StructField("part_id", StringType(), True),
                        StructField("tenant_id", StringType(), True),
                        StructField("part_number", StringType(), True),
                        StructField("category", StringType(), True),
                        StructField("lifecycle_status", StringType(), True),
                        StructField("default_supplier_id", StringType(), True),
                        StructField("qualified_supplier_ids",
                                    StringType(), True),
                        StructField("unit_cost", StringType(), True),
                        StructField("moq", StringType(), True),
                        StructField("lead_time_days_avg", StringType(), True),
                        StructField("lead_time_days_p95", StringType(), True),
                        StructField("source_timestamp", StringType(), True)
                    ])

                    parts_df = parts_df.select(
                        from_json(col("data"), parts_schema).alias(
                            "parsed_data"),
                        "_ab_source_file_url",
                        "_airbyte_extracted_at"
                    ).select("parsed_data.*", "_ab_source_file_url", "_airbyte_extracted_at")
                else:
                    print("Parts data column still NULL - schema fix didn't work")
                    parts_df = None
            else:
                print("Parts data appears to be flattened correctly")

            if parts_df is not None:
                parts_count = parts_df.count()
                print(f"Parts loaded: {parts_count:,}")

        except Exception as e:
            print(f"Failed to read parts: {str(e)[:100]}...")
            parts_df = None

        return suppliers_df, parts_df

    def validate_suppliers(self, df):
        """
        Apply comprehensive supplier validation per requirements:
        - PK uniqueness
        - Range validations (on_time_delivery_rate 0-100, risk_score 0-100)
        - Allowed values (financial_risk_tier, approved_status)
        - Required fields

        Args:
            df (DataFrame): Supplier DataFrame.

        Returns:
            DataFrame: Validated supplier DataFrame with dq_violations and is_valid columns.
        """
        print("Validating suppliers...")

        # Add data quality validation columns
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

                # Range validations per PDF specs - cast strings to double first
                when((col("on_time_delivery_rate").cast("double") < 0) | (col("on_time_delivery_rate").cast("double") > 100),
                     lit("INVALID_ON_TIME_RATE")).otherwise(lit(None)),
                when((col("risk_score").cast("double") < 0) | (col("risk_score").cast("double") > 100),
                     lit("INVALID_RISK_SCORE")).otherwise(lit(None)),
                when(col("lead_time_days_avg").cast("int") < 0, lit(
                    "NEGATIVE_LEAD_TIME")).otherwise(lit(None)),
                when(col("defect_rate_ppm").cast("int") < 0, lit(
                    "NEGATIVE_DEFECT_RATE")).otherwise(lit(None)),

                # Allowed value validations
                when(~col("financial_risk_tier").isin(["LOW", "MEDIUM", "HIGH"]),
                     lit("INVALID_FINANCIAL_TIER")).otherwise(lit(None)),
                when(~col("approved_status").isin(["PENDING", "APPROVED", "SUSPENDED", "BLACKLISTED"]),
                     lit("INVALID_STATUS")).otherwise(lit(None)),

                # Country code validation (ISO-3166-1 alpha-2)
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

        # Check for PK duplicates
        pk_dupes = validated_df.groupBy(
            "supplier_id").count().filter(col("count") > 1).count()
        if pk_dupes > 0:
            print(f"WARNING: {pk_dupes} duplicate supplier_ids found")

        # Show top violations if any
        if valid < total:
            print("Top data quality violations:")
            validated_df.filter(~col("is_valid")) \
                .select(explode("dq_violations").alias("violation")) \
                .groupBy("violation").count() \
                .orderBy(desc("count")) \
                .show(5, truncate=False)

        return validated_df

    def validate_parts(self, parts_df, valid_supplier_ids):
        """
        Apply parts validation with referential integrity checks:
        - FK integrity (default_supplier_id, qualified_supplier_ids)
        - Business rule validations
        - Required field checks

        Args:
            parts_df (DataFrame): Parts DataFrame.
            valid_supplier_ids (list): List of valid supplier IDs.

        Returns:
            DataFrame: Validated parts DataFrame with dq_violations and is_valid columns.
        """
        print("Validating parts with referential integrity...")

        # Broadcast valid supplier IDs for efficient lookup
        supplier_ids_broadcast = self.spark.sparkContext.broadcast(
            set(valid_supplier_ids))

        def check_supplier_references(default_id, qualified_ids_str):
            """UDF to validate supplier foreign keys"""
            valid_ids = supplier_ids_broadcast.value
            violations = []

            # Check default supplier FK
            if not default_id:
                violations.append("MISSING_DEFAULT_SUPPLIER")
            elif default_id not in valid_ids:
                violations.append("ORPHAN_DEFAULT_SUPPLIER")

            # Check qualified suppliers FKs
            if qualified_ids_str:
                try:
                    # Handle array-like string format or comma-separated
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

        # Apply comprehensive validation
        validated_df = parts_df.withColumn(
            "dq_violations",
            array(
                # Primary key and required fields - wrap single strings in arrays
                when(col("part_id").isNull(), array(
                    lit("MISSING_PART_ID"))).otherwise(array()),
                when(col("tenant_id").isNull(), array(
                    lit("MISSING_TENANT_ID"))).otherwise(array()),
                when(col("part_number").isNull(), array(
                    lit("MISSING_PART_NUMBER"))).otherwise(array()),
                when(col("category").isNull(), array(
                    lit("MISSING_CATEGORY"))).otherwise(array()),

                # Referential integrity checks (already returns array or null)
                when(check_refs_udf(col("default_supplier_id"), col("qualified_supplier_ids")).isNotNull(),
                     check_refs_udf(col("default_supplier_id"), col("qualified_supplier_ids"))).otherwise(array()),

                # Business rule validations - wrap in arrays
                when(col("unit_cost").cast("double") < 0, array(
                    lit("NEGATIVE_UNIT_COST"))).otherwise(array()),
                when(col("moq").cast("int") < 0, array(
                    lit("NEGATIVE_MOQ"))).otherwise(array()),
                when(col("lead_time_days_avg").cast("int") < 0, array(
                    lit("NEGATIVE_LEAD_TIME"))).otherwise(array()),

                # Category validation - wrap in arrays
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

        # Calculate validation metrics
        total = validated_df.count()
        valid = validated_df.filter(col("is_valid")).count()
        pass_rate = (valid / total * 100) if total > 0 else 0

        print(
            f"Parts validation: {valid:,}/{total:,} valid ({pass_rate:.1f}% pass rate)")

        # Referential integrity stats
        orphan_count = validated_df.filter(
            array_contains(col("dq_violations"), "ORPHAN_DEFAULT_SUPPLIER") |
            array_contains(col("dq_violations"), "ORPHAN_QUALIFIED_SUPPLIER")
        ).count()

        if orphan_count > 0:
            print(
                f"Referential integrity violations: {orphan_count:,} parts with invalid supplier references")

        # Show top violations
        if valid < total:
            print("Top data quality violations:")
            validated_df.filter(~col("is_valid")) \
                .select(explode("dq_violations").alias("violation")) \
                .groupBy("violation").count() \
                .orderBy(desc("count")) \
                .show(5, truncate=False)

        return validated_df

    def create_iceberg_tables(self):
        """
        Create Iceberg tables with proper schema per requirements.
        """
        print("Creating Iceberg tables...")

        # Create suppliers table per PDF schema
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS cdf.dim_suppliers_v1 (
                supplier_id STRING,
                supplier_code STRING,
                tenant_id STRING,
                legal_name STRING,
                country STRING,
                on_time_delivery_rate DECIMAL(5,2),
                risk_score DECIMAL(5,2),
                financial_risk_tier STRING,
                defect_rate_ppm INT,
                lead_time_days_avg INT,
                lead_time_days_p95 INT,
                approved_status STRING,
                certifications ARRAY<STRING>,
                source_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                dq_violations ARRAY<STRING>,
                is_valid BOOLEAN,
                dq_timestamp TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (tenant_id, bucket(16, supplier_id))
            TBLPROPERTIES (
                'write.parquet.compression-codec' = 'zstd',
                'write.metadata.delete-after-commit.enabled' = 'true'
            )
        """)

        # Create parts table per PDF schema
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS cdf.dim_parts_v1 (
                part_id STRING,
                tenant_id STRING,
                part_number STRING,
                category STRING,
                lifecycle_status STRING,
                default_supplier_id STRING,
                qualified_supplier_ids STRING,
                unit_cost DECIMAL(18,6),
                moq INT,
                lead_time_days_avg INT,
                lead_time_days_p95 INT,
                source_timestamp TIMESTAMP,
                ingestion_timestamp TIMESTAMP,
                dq_violations ARRAY<STRING>,
                is_valid BOOLEAN,
                dq_timestamp TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (tenant_id, category, bucket(16, part_id))
            TBLPROPERTIES (
                'write.parquet.compression-codec' = 'zstd',
                'write.metadata.delete-after-commit.enabled' = 'true'
            )
        """)

        print("Iceberg tables created/verified")

    def write_to_iceberg(self, suppliers_df, parts_df=None):
        """
        Write validated data to Iceberg tables.

        Args:
            suppliers_df (DataFrame): Validated suppliers DataFrame.
            parts_df (DataFrame): Validated parts DataFrame (optional).
        """
        print("Writing to Iceberg tables...")

        # Write suppliers with proper column selection and type casting
        suppliers_final = suppliers_df.select(
            "supplier_id",
            "supplier_code",
            "tenant_id",
            "legal_name",
            "country",
            col("on_time_delivery_rate").cast(
                "decimal(5,2)").alias("on_time_delivery_rate"),
            col("risk_score").cast("decimal(5,2)").alias("risk_score"),
            "financial_risk_tier",
            col("defect_rate_ppm").cast("int").alias("defect_rate_ppm"),
            col("lead_time_days_avg").cast("int").alias("lead_time_days_avg"),
            col("lead_time_days_p95").cast("int").alias("lead_time_days_p95"),
            "approved_status",
            split(col("certifications"), ",").alias(
                "certifications"),  # Convert string to array
            col("source_timestamp").cast(
                "timestamp").alias("source_timestamp"),
            current_timestamp().alias("ingestion_timestamp"),
            "dq_violations",
            "is_valid",
            "dq_timestamp"
        )

        suppliers_final.writeTo("cdf.dim_suppliers_v1").append()
        print("Suppliers written to Iceberg")

        # Write parts if available
        if parts_df is not None:
            parts_final = parts_df.select(
                "part_id",
                "tenant_id",
                "part_number",
                "category",
                "lifecycle_status",
                "default_supplier_id",
                "qualified_supplier_ids",
                col("unit_cost").cast("decimal(18,6)").alias("unit_cost"),
                col("moq").cast("int").alias("moq"),
                col("lead_time_days_avg").cast(
                    "int").alias("lead_time_days_avg"),
                col("lead_time_days_p95").cast(
                    "int").alias("lead_time_days_p95"),
                col("source_timestamp").cast(
                    "timestamp").alias("source_timestamp"),
                current_timestamp().alias("ingestion_timestamp"),
                "dq_violations",
                "is_valid",
                "dq_timestamp"
            )

            parts_final.writeTo("cdf.dim_parts_v1").append()
            print("Parts written to Iceberg")
        else:
            print("Parts skipped - not processed")

        print("Data written to Iceberg warehouse")

    def run_pipeline(self, run_date="2025-09-10"):
        """
        Execute complete data quality pipeline.

        Args:
            run_date (str): Date string for partitioned S3 data.

        Returns:
            bool: True if pipeline succeeded, False otherwise.
        """
        start_time = datetime.now()
        print(f"Starting supply chain data pipeline for {run_date}")

        try:
            # 1. Read source data
            suppliers_df, parts_df = self.read_source_data(run_date)

            if suppliers_df is None:
                print("Pipeline aborted - no supplier data")
                return False

            # 2. Create Iceberg tables
            self.create_iceberg_tables()

            # 3. Validate suppliers
            validated_suppliers = self.validate_suppliers(suppliers_df)

            # 4. Handle parts if available
            if parts_df is not None:
                # Extract valid supplier IDs for FK validation
                valid_supplier_ids = (
                    validated_suppliers
                    .filter(col("is_valid"))
                    .select("supplier_id")
                    .rdd.map(lambda row: row[0])
                    .collect()
                )

                print(
                    f"Valid suppliers for FK checks: {len(valid_supplier_ids):,}")
                validated_parts = self.validate_parts(
                    parts_df, valid_supplier_ids)
            else:
                print(
                    "Parts processing skipped - data not available or schema not fixed")
                validated_parts = None

            # 5. Write to Iceberg
            self.write_to_iceberg(validated_suppliers, validated_parts)

            # 6. Pipeline summary
            elapsed = datetime.now() - start_time
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

                # Check if we meet 99% target
                overall_pass_rate = ((supplier_stats['valid'] + parts_stats['valid']) /
                                     (supplier_stats['total'] + parts_stats['total'])) * 100
                print(f"  Overall pass rate: {overall_pass_rate:.1f}%")
                print(
                    f"  Target: 99% pass rate - {'PASS' if overall_pass_rate >= 99.0 else 'FAIL'}")
            else:
                print(f"  Parts: SKIPPED")
                supplier_pass_rate = (
                    supplier_stats['valid']/supplier_stats['total']*100)
                print(f"  Supplier pass rate: {supplier_pass_rate:.1f}%")

            # Verify data was written
            print("\nVerifying data in Iceberg tables:")
            supplier_count = self.spark.sql(
                "SELECT COUNT(*) as count FROM cdf.dim_suppliers_v1").collect()[0]['count']
            print(f"  Suppliers in Iceberg: {supplier_count:,}")

            if validated_parts is not None:
                parts_count = self.spark.sql(
                    "SELECT COUNT(*) as count FROM cdf.dim_parts_v1").collect()[0]['count']
                print(f"  Parts in Iceberg: {parts_count:,}")

            return True

        except Exception as e:
            print(f"Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def stop(self):
        """
        Clean shutdown of Spark session.
        """
        self.spark.stop()
        print("Spark session stopped")


if __name__ == "__main__":
    pipeline = SupplyChainDataPipeline()

    try:
        success = pipeline.run_pipeline("2025-09-10")
        exit_code = 0 if success else 1
    except Exception as e:
        print(f"Pipeline error: {e}")
        exit_code = 1
    finally:
        pipeline.stop()

    exit(exit_code)
