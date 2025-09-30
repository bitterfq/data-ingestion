#!/bin/bash
set -e

echo "Cleaning up AWS Glue and S3 resources..."

# Delete all Glue tables if database exists
if aws glue get-database --name supply_chain >/dev/null 2>&1; then
  TABLES=$(aws glue get-tables --database-name supply_chain --query 'TableList[].Name' --output text || true)
  if [ -n "$TABLES" ]; then
    for table in $TABLES; do
      echo "Deleting Glue table: $table"
      aws glue delete-table --database-name supply_chain --name "$table" || true
    done
  else
    echo "No Glue tables found in supply_chain"
  fi

  echo "Deleting Glue database: supply_chain"
  aws glue delete-database --name supply_chain || true
else
  echo "Glue database supply_chain does not exist, skipping"
fi

# Remove S3 buckets (ignore if missing/empty)
for bucket in cdf-upload cdf-raw cdf-silver; do
  echo "Removing all objects from s3://$bucket/ (if exists)"
  aws s3 rm "s3://$bucket/" --recursive >/dev/null 2>&1 || true
done

# Clean Postgres tables only if container is running
if docker ps --format '{{.Names}}' | grep -q '^supply-chain-postgres$'; then
  echo "Cleaning Postgres tables"
  docker exec -i supply-chain-postgres psql -U pipeline_user -d supply_chain -c "DELETE FROM parts; DELETE FROM suppliers;" || true
else
  echo "Postgres container supply-chain-postgres not running, skipping"
fi

echo "Cleanup completed."
