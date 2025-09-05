#!/bin/bash
# setup_pipeline.sh - Automated setup for event-driven data pipeline

set -e  # Exit on any error

echo "Setting up event-driven data pipeline..."

# Step 1: Start containers
echo "Starting Docker containers..."
docker compose up -d

# Step 2: Wait for services to be ready
echo "Waiting for services to start..."
sleep 15

# Step 3: Create Kafka topic
echo "Creating Kafka topic..."
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic file-events --partitions 1 --replication-factor 1 || echo "Topic already exists"

# Step 4: Configure MinIO alias
echo "Setting up MinIO alias..."
docker exec -it minio mc alias set local http://localhost:9000 minioadmin minioadmin

# Step 5: Create buckets
echo "Creating MinIO buckets..."
docker exec -it minio mc mb local/raw --ignore-existing
docker exec -it minio mc mb local/processed --ignore-existing

# Step 6: Configure Kafka notification target
echo "Configuring Kafka notifications..."
docker exec -it minio mc admin config set local notify_kafka:kafka brokers="kafka:9092" topic="file-events"

# Step 7: Enable the notification target
echo "Enabling Kafka notifications..."
docker exec -it minio mc admin config set local notify_kafka:kafka enable=on

# Step 8: Restart MinIO to apply config
echo "Restarting MinIO..."
docker compose restart minio
sleep 5

# Step 9: Set up bucket event notification
echo "Setting up bucket notifications..."
docker exec -it minio mc event add local/raw arn:minio:sqs::kafka:kafka --event put

# Step 10: Verify setup
echo "Verifying setup..."
echo "✓ Kafka topics:"
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list

echo "✓ MinIO buckets:"
docker exec -it minio mc ls local

echo "✓ Kafka notification config:"
docker exec -it minio mc admin config get local notify_kafka:kafka

echo "✓ Bucket events:"
docker exec -it minio mc event list local/raw

echo ""
echo "🎉 Setup complete!"
echo ""
echo "To test the pipeline:"
echo "1. Start Kafka consumer: docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic file-events --from-beginning"
echo "2. Run generator: python3 data_generator/generator.py"
echo "3. Watch Kafka messages appear in the consumer"
echo ""
echo "Services available:"
echo "- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "- Kafka UI: http://localhost:8080"
echo ""