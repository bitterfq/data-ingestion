# MinIO Kafka Event Notifications Setup

Documentation for configuring MinIO S3 event notifications to publish to Kafka topics.

## Architecture

```
File Upload → MinIO Bucket → S3 Event → Kafka Topic → Consumer Applications
```

## Prerequisites

- Kafka cluster running and accessible
- MinIO instance with admin access
- Docker environment with containers on same network

## Kafka Setup

### 1. Create Kafka Topic

```bash
# Create topic for file events
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --topic file-events --partitions 1 --replication-factor 1

# Verify topic creation
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
```

### 2. Test Kafka Connectivity

```bash
# Start consumer to monitor messages
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic file-events --from-beginning
```

## MinIO Configuration

### 1. Set MinIO Alias

```bash
# Configure mc client to connect to MinIO
docker exec -it minio mc alias set local http://localhost:9000 minioadmin minioadmin
```

### 2. Create Buckets

```bash
# Create source bucket for file uploads
docker exec -it minio mc mb local/raw --ignore-existing
docker exec -it minio mc mb local/processed --ignore-existing
```

### 3. Configure Kafka Notification Target

```bash
# Set up Kafka notification configuration
docker exec -it minio mc admin config set local notify_kafka:kafka brokers="kafka:9092" topic="file-events"

# Enable the notification target
docker exec -it minio mc admin config set local notify_kafka:kafka enable=on

# Restart MinIO to apply configuration
docker compose restart minio
```

### 4. Verify Kafka Target Configuration

```bash
# Check if Kafka target is properly configured
docker exec -it minio mc admin config get local notify_kafka:kafka
```

Expected output:
```
notify_kafka:kafka topic=file-events brokers=kafka:9092 sasl_username= sasl_password= ... enable=on
```

### 5. Set Up Bucket Event Notification

```bash
# Configure bucket to send events to Kafka
docker exec -it minio mc event add local/raw arn:minio:sqs::kafka:kafka --event put

# Verify event configuration
docker exec -it minio mc event list local/raw
```

## Event Message Format

When files are uploaded to MinIO, Kafka receives S3-compatible event messages:

```json
{
  "EventName": "s3:ObjectCreated:Put",
  "Key": "raw/tenant_acme/2025-09-05/20250905_123058/suppliers.csv",
  "Records": [{
    "eventVersion": "2.0",
    "eventSource": "minio:s3",
    "eventTime": "2025-09-05T19:30:58.178Z",
    "eventName": "s3:ObjectCreated:Put",
    "s3": {
      "bucket": {"name": "raw"},
      "object": {
        "key": "tenant_acme%2F2025-09-05%2F20250905_123058%2Fsuppliers.csv",
        "size": 3428639,
        "eTag": "f93768a488819b6a425184af0333217d"
      }
    }
  }]
}
```

## Testing the Integration

1. **Start Kafka consumer**:
   ```bash
   docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic file-events --from-beginning
   ```

2. **Upload test file**:
   ```bash
   docker exec -it minio mc cp /etc/hosts local/raw/test-file.txt
   ```

3. **Verify message received**: Check consumer output for JSON event message

## Docker Compose Configuration

MinIO service configuration for Kafka integration:

```yaml
minio:
  container_name: minio
  image: quay.io/minio/minio:latest
  environment:
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin
    # Optional: Set via environment variables
    MINIO_NOTIFY_KAFKA_ENABLE_kafka: "on"
    MINIO_NOTIFY_KAFKA_BROKERS_kafka: "kafka:9092"
    MINIO_NOTIFY_KAFKA_TOPIC_kafka: "file-events"
  depends_on:
    - kafka
  networks:
    - data_pipeline
```

Note: Environment variable configuration is an alternative to `mc admin config` commands, but manual configuration via mc is more reliable for initial setup.