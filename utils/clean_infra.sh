#!/bin/bash
set -e

echo "Cleaning up AWS Glue and S3 resources..."

# Delete all Glue tables before deleting the database
TABLES=$(aws glue get-tables --database-name supply_chain --query 'TableList[].Name' --output text)
for table in $TABLES; do
  echo "Deleting Glue table: $table"
  aws glue delete-table --database-name supply_chain --name "$table"
done

# Delete the Glue database (ignore error if not present)
aws glue delete-database --name supply_chain || true

# Remove S3 buckets (ignore error if not present)
for bucket in cdf-upload cdf-raw cdf-silver; do
  echo "Removing all objects from s3://$bucket/"
  aws s3 rm "s3://$bucket/" --recursive || true
done

docker exec -it supply-chain-postgres psql -U pipeline_user -d supply_chain -c "DELETE FROM parts; DELETE FROM suppliers;"

echo "Cleanup completed."