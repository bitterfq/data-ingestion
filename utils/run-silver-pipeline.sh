#!/bin/bash

set -e

# Configuration
SPARK_PACKAGES="\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.iceberg:iceberg-aws:1.5.2,\
software.amazon.awssdk:bundle:2.20.160,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262"

ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE:-"s3a://cdf-silver/warehouse/"}
RUN_DATE=${1:-$(date +%Y-%m-%d)}

echo "Running Silver Pipeline for date: $RUN_DATE"
echo "Iceberg Warehouse: $ICEBERG_WAREHOUSE"

# Clean up any existing local warehouse
rm -rf ./spark-warehouse

spark-submit \
  --packages $SPARK_PACKAGES \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.defaultCatalog=glue_catalog \
  --conf spark.sql.warehouse.dir=/tmp/spark-warehouse \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  utils/spark_silver_pipeline.py $RUN_DATE

echo "Pipeline completed successfully!"