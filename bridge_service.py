"""
Kafka-to-Airbyte Bridge Service

Listens to Kafka for MinIO file event notifications, processes new CSV files
uploaded to the 'raw' MinIO bucket, converts them to Parquet format, and uploads
the results to the 'processed' MinIO bucket. Designed to automate ingestion and
format conversion for downstream data pipelines (e.g., Airbyte).

Key Features:
- Consumes file event messages from Kafka.
- Parses MinIO event structure to extract file metadata.
- Downloads CSV files from MinIO, converts to Parquet using pandas.
- Uploads Parquet files to a separate MinIO bucket.
- Logs all major actions and errors for observability.
"""

import json
import logging
import os
import time
from kafka import KafkaConsumer
import requests
from minio import Minio
import pandas as pd
from io import BytesIO
from urllib.parse import unquote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaAirbyteBridge:
    """
    Bridge service that connects Kafka file event notifications to Airbyte ingestion.

    Responsibilities:
    - Consumes file event messages from a Kafka topic.
    - Parses MinIO event payloads to extract file and tenant information.
    - Downloads CSV files from MinIO, converts them to Parquet, and uploads to a processed bucket.
    - Handles errors gracefully and logs processing steps.
    """

    def __init__(self):
        """
        Initialize KafkaAirbyteBridge.

        - Reads configuration from environment variables.
        - Sets up Kafka consumer for file event topic.
        - Initializes MinIO client for file operations.
        - Logs startup information.
        """

        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'file-events')
        self.airbyte_api_url = os.getenv('AIRBYTE_API_URL', 'http://airbyte:8001/api/v1')
        
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=[self.kafka_bootstrap_servers],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='airbyte-trigger-group',
            auto_offset_reset='latest'
        )
        self.minio_client = Minio(
            'minio:9000',
            access_key='minioadmin', 
            secret_key='minioadmin',
            secure=False
        )    
        logger.info(f"Bridge service started - consuming from {self.kafka_topic}")
    
    def parse_minio_event(self, event):
        """
        Parse a MinIO event notification from Kafka.

        Args:
            event (dict): The event payload from Kafka.

        Returns:
            dict or None: Parsed file info including bucket, tenant_id, run_id, entity_type, and s3_path,
                          or None if parsing fails or event is not relevant.
        """
        try:
            records = event.get('Records', [])
            for record in records:
                s3 = record.get('s3', {})
                bucket = s3.get('bucket', {}).get('name', '')
                object_key = unquote(s3.get('object', {}).get('key', ''))
                
                # Parse: tenant_acme/2025-09-05/20250905_123058/suppliers.csv
                parts = object_key.split('/')
                if len(parts) >= 4:
                    entity_type = 'suppliers' if 'suppliers' in parts[3] else 'parts' if 'parts' in parts[3] else None
                    if entity_type:
                        return {
                            'bucket': bucket,
                            'tenant_id': parts[0],
                            'run_id': parts[2], 
                            'entity_type': entity_type,
                            's3_path': f's3://{bucket}/{object_key}'
                        }
            return None
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None
    
    def process_file(self, file_info):
        """
        Process a single file event.

        - Downloads the CSV file from the raw MinIO bucket.
        - Converts the CSV to a pandas DataFrame.
        - Writes the DataFrame to Parquet format in memory.
        - Uploads the Parquet file to the processed MinIO bucket.

        Args:
            file_info (dict): Metadata about the file to process.

        Returns:
            bool: True if processing succeeds, False otherwise.
        """
        try:
            source_bucket = file_info['bucket']
            source_key = file_info['s3_path'].replace(f's3://{source_bucket}/', '')
            
            # Destination path in processed bucket
            # raw/tenant_acme/2025-09-05/20250905_145143/suppliers.csv
            # -> processed/tenant_acme/2025-09-05/20250905_145143/suppliers.parquet
            dest_key = source_key.replace('.csv', '.parquet')
            dest_bucket = 'raw'
            
            logger.info(f"Processing {file_info['entity_type']} from {file_info['tenant_id']}")
            logger.info(f"  Source: s3://{source_bucket}/{source_key}")
            logger.info(f"  Destination: s3://{dest_bucket}/{dest_key}")
            
            # Download CSV from MinIO
            logger.info("  Downloading CSV...")
            csv_response = self.minio_client.get_object(source_bucket, source_key)
            csv_data = csv_response.data.decode('utf-8')
            
            # Convert CSV to DataFrame
            logger.info("  Converting CSV to DataFrame...")
            from io import StringIO
            df = pd.read_csv(StringIO(csv_data))
            logger.info(f"  Loaded {len(df)} records")
            
            # Convert to Parquet
            logger.info("  Converting to Parquet...")
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, compression='snappy')
            parquet_buffer.seek(0)
            
            # Upload Parquet to processed bucket
            logger.info("  Uploading Parquet...")
            self.minio_client.put_object(
                dest_bucket,
                dest_key,
                data=parquet_buffer,
                length=len(parquet_buffer.getvalue()),
                content_type='application/octet-stream'
            )
            
            logger.info(f"✓ Successfully processed {file_info['entity_type']}")
            logger.info(f"  Converted {len(df)} records from CSV to Parquet")
            return True
            
        except Exception as e:
            logger.error(f"✗ Error processing {file_info['entity_type']}: {e}")
            return False
    
    def run(self):
        """
        Main event loop.

        - Waits for Kafka messages.
        - For each message, parses and processes the file event.
        - Handles graceful shutdown on KeyboardInterrupt.
        """
        logger.info("Waiting for Kafka events...")
        
        try:
            for message in self.consumer:
                file_info = self.parse_minio_event(message.value)
                if file_info:
                    self.process_file(file_info)
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    bridge = KafkaAirbyteBridge()
    bridge.run()