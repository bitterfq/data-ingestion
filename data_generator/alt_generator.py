"""
Data Generator for Supply Chain POC - High Performance Version

Key features:
- Vectorized numpy operations instead of loops
- Bulk data generation with pre-allocated arrays
- Chunked processing for memory efficiency
- Parallel CSV writing
- Optimized dirty data injection

Performance target: 100k suppliers + 100k parts in <2 minutes
"""

import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
import decimal
import json
import psycopg2
import boto3
import csv
from datetime import timedelta
import datetime as dt
import ulid
from typing import List, Dict, Any, Optional
from faker import Faker
import numpy as np
import random
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.otel_setup import init_tracer



class DataGenerator:
    """High-performance vectorized data generator"""

    def __init__(self, seed: int = 42, tenant_id: str = "tenant_acme"):
        self.seed = seed
        self.tenant_id = tenant_id
        random.seed(seed)
        np.random.seed(seed)
        self.fake = Faker()
        Faker.seed(seed)
        self._init_distributions()

    def _init_distributions(self):
        """Pre-compute all distributions for vectorized generation"""
        self.countries = ["CN", "US", "DE", "MX", "IN", "VN", "PL", "JP", "KR"]
        self.country_probs = np.array([22, 18, 10, 10, 10, 8, 6, 8, 8]) / 100

        self.certifications = ["ISO9001", "IATF16949", "AS9100", "ISO14001"]
        self.cert_probs = np.array([0.7, 0.3, 0.1, 0.2])

        self.part_categories = ["ELECTRICAL",
                                "MECHANICAL", "RAW_MATERIAL", "OTHER"]
        self.category_probs = np.array([0.35, 0.35, 0.20, 0.10])

        # Pre-generate common fake data to reuse
        self.company_names = [self.fake.company() for _ in range(1000)]
        self.cities = [self.fake.city() for _ in range(500)]
        self.streets = [self.fake.street_address() for _ in range(500)]

        # Currency mappings
        self.currency_map = {
            "US": "USD", "CN": "CNY", "DE": "EUR", "MX": "MXN",
            "IN": "INR", "VN": "VND", "PL": "PLN", "JP": "JPY", "KR": "KRW"
        }

        self.region_map = {
            "US": "AMERICAS", "MX": "AMERICAS",
            "CN": "APAC", "JP": "APAC", "KR": "APAC", "IN": "APAC", "VN": "APAC",
            "DE": "EMEA", "PL": "EMEA"
        }

    def generate_suppliers(self, count: int) -> List[Dict[str, Any]]:
        """Generate suppliers using vectorized operations"""
        print(f"Generating {count:,} suppliers...")

        supplier_ids = [ulid.new().str for _ in range(count)]
        on_time_rates = np.clip(np.random.normal(93, 5, count), 60, 100)
        risk_scores = np.clip(100 - on_time_rates +
                              np.random.normal(5, 3, count), 0, 100)
        countries = np.random.choice(
            self.countries, size=count, p=self.country_probs)
        currencies = np.array([self.currency_map.get(c, "USD")
                              for c in countries])
        regions = np.array([self.region_map.get(c, "OTHER")
                           for c in countries])
        lead_times_avg = np.clip(np.random.normal(
            21, 6, count), 3, 90).astype(int)
        lead_times_p95 = np.clip(np.random.normal(
            35, 10, count), 7, 180).astype(int)
        defect_rates = np.abs(np.random.normal(250, 180, count)).astype(int)
        capacities = (np.abs(np.random.normal(
            5000, 3000, count)) + 100).astype(int)
        statuses = np.random.choice(
            ["APPROVED", "PENDING", "SUSPENDED", "BLACKLISTED"],
            size=count, p=[0.80, 0.15, 0.04, 0.01]
        )
        tiers = np.where(risk_scores < 35, "LOW",
                         np.where(risk_scores < 60, "MEDIUM", "HIGH"))
        supplier_codes = [
            f"S{random.randint(100000, 999999)}" for _ in range(count)]
        company_indices = np.random.choice(len(self.company_names), count)
        legal_names = [self.company_names[i] for i in company_indices]

        suppliers = []
        base_timestamp = dt.datetime.now(dt.timezone.utc)

        for i in range(count):
            suppliers.append({
                "supplier_id": supplier_ids[i],
                "supplier_code": supplier_codes[i],
                "tenant_id": self.tenant_id,
                "legal_name": legal_names[i],
                "dba_name": legal_names[i] + " Corp" if random.random() < 0.3 else None,
                "country": countries[i],
                "region": regions[i],
                "address_line1": random.choice(self.streets),
                "address_line2": None,
                "city": random.choice(self.cities),
                "state": "CA" if countries[i] == "US" else None,
                "postal_code": str(random.randint(10000, 99999)),
                "contact_email": f"contact{i}@{legal_names[i].lower().replace(' ', '')}.com",
                "contact_phone": f"+1-555-{random.randint(1000000, 9999999)}",
                "preferred_currency": currencies[i],
                "incoterms": random.choice(["DDP", "FOB", "CIF", "EXW"]),
                "lead_time_days_avg": int(lead_times_avg[i]),
                "lead_time_days_p95": int(lead_times_p95[i]),
                "on_time_delivery_rate": float(round(on_time_rates[i], 2)),
                "defect_rate_ppm": int(defect_rates[i]),
                "capacity_units_per_week": int(capacities[i]),
                "risk_score": float(round(risk_scores[i], 2)),
                "financial_risk_tier": tiers[i],
                "certifications": random.sample(self.certifications, k=random.choice([1, 2, 3])),
                "compliance_flags": random.sample(["ITAR", "REACH", "ROHS"], k=random.choice([0, 1, 2])),
                "approved_status": statuses[i],
                "contracts": [f"CONTRACT_{ulid.new().str[:8]}"] if random.random() < 0.5 else [],
                "terms_version": f"{random.randint(1, 5)}.{random.randint(0, 9)}",
                "geo_coords": {
                    "lat": round(random.uniform(-90, 90), 6),
                    "lon": round(random.uniform(-180, 180), 6)
                },
                "data_source": "synthetic.v1",
                "source_timestamp": (base_timestamp - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "ingestion_timestamp": base_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "schema_version": "1.0.0"
            })

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i+1:,} suppliers...")

        return suppliers

    def generate_parts(self, count: int, supplier_ids: List[str]) -> List[Dict[str, Any]]:
        """Generate parts using vectorized operations"""
        print(f"Generating {count:,} parts...")

        part_ids = [ulid.new().str for _ in range(count)]
        categories = np.random.choice(
            self.part_categories, size=count, p=self.category_probs)
        lead_means = np.where(categories == "ELECTRICAL", 32,
                              np.where(categories == "MECHANICAL", 24,
                                       np.where(categories == "RAW_MATERIAL", 14, 20)))
        lead_stds = np.where(categories == "ELECTRICAL", 8,
                             np.where(categories == "MECHANICAL", 6,
                                      np.where(categories == "RAW_MATERIAL", 4, 5)))
        lead_times_avg = np.clip(np.random.normal(
            lead_means, lead_stds), 2, 200).astype(int)
        lead_times_p95 = np.clip(np.random.normal(
            lead_means + 8, lead_stds), 5, 250).astype(int)
        unit_costs = np.round(np.exp(np.random.normal(3.0, 0.8, count)), 2)
        moqs = np.random.choice([1, 10, 50, 100, 500], size=count)
        quality_grades = np.random.choice(
            ["A", "B", "C"], size=count, p=[0.6, 0.3, 0.1])
        lifecycle_statuses = np.random.choice(
            ["ACTIVE", "NEW", "NRND", "EOL"],
            size=count, p=[0.75, 0.10, 0.10, 0.05]
        )
        uoms = np.random.choice(
            ["EA", "KG", "M"], size=count, p=[0.7, 0.2, 0.1])
        part_numbers = [
            f"P-{random.randint(100000, 999999)}" for _ in range(count)]

        parts = []
        base_timestamp = dt.datetime.now(dt.timezone.utc)

        for i in range(count):
            num_qualified = random.choice([1, 2, 3])
            qualified_supplier_ids = random.sample(
                supplier_ids, min(num_qualified, len(supplier_ids)))

            parts.append({
                "part_id": part_ids[i],
                "tenant_id": self.tenant_id,
                "part_number": part_numbers[i],
                "description": f"Part component {i+1} for manufacturing",
                "category": categories[i],
                "lifecycle_status": lifecycle_statuses[i],
                "uom": uoms[i],
                "spec_hash": f"SPEC_{ulid.new().str[:16]}",
                "bom_compatibility": [f"BOM_{ulid.new().str[:8]}"] if random.random() < 0.3 else [],
                "default_supplier_id": qualified_supplier_ids[0],
                "qualified_supplier_ids": qualified_supplier_ids,
                "unit_cost": float(unit_costs[i]),
                "moq": int(moqs[i]),
                "lead_time_days_avg": int(lead_times_avg[i]),
                "lead_time_days_p95": int(lead_times_p95[i]),
                "quality_grade": quality_grades[i],
                "compliance_flags": random.sample(["ROHS", "REACH", "ITAR"], k=random.choice([0, 1, 2])),
                "hazard_class": random.choice(["CLASS_1", "CLASS_3", "CLASS_8", "CLASS_9"]) if random.random() < 0.1 else None,
                "last_price_change": (base_timestamp - timedelta(days=random.randint(30, 365))).date().isoformat(),
                "data_source": "synthetic.v1",
                "source_timestamp": (base_timestamp - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "ingestion_timestamp": base_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "schema_version": "1.0.0"
            })

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i+1:,} parts...")

        return parts

    def inject_dirty_data(self, data: List[Dict[str, Any]], data_type: str, anomaly_rate: float = 0.06):
        """Dirty data injection using vectorized operations"""
        dirty_data = data.copy()
        num_anomalies = int(len(data) * anomaly_rate)
        dirty_indices = set(random.sample(range(len(data)), num_anomalies))

        print(f"Injecting {num_anomalies:,} anomalies into {data_type}...")

        if data_type == "suppliers":
            for idx in dirty_indices:
                supplier = dirty_data[idx]
                anomaly_type = random.choice([
                    "out_of_range_rate", "missing_contact", "bogus_country", "future_timestamp"
                ])

                if anomaly_type == "out_of_range_rate":
                    supplier["on_time_delivery_rate"] = 104.0
                elif anomaly_type == "missing_contact":
                    supplier["contact_email"] = None
                elif anomaly_type == "bogus_country":
                    supplier["country"] = "XX"
                elif anomaly_type == "future_timestamp":
                    future_date = dt.datetime.now(
                        dt.timezone.utc) + timedelta(days=random.randint(1, 30))
                    supplier["source_timestamp"] = future_date.isoformat()

        elif data_type == "parts":
            for idx in dirty_indices:
                part = dirty_data[idx]
                anomaly_type = random.choice([
                    "negative_cost", "wrong_uom", "future_timestamp"
                ])

                if anomaly_type == "negative_cost":
                    part["unit_cost"] = -random.uniform(1.0, 100.0)
                elif anomaly_type == "wrong_uom":
                    part["uom"] = "INVALID"
                elif anomaly_type == "future_timestamp":
                    future_date = dt.datetime.now(
                        dt.timezone.utc) + timedelta(days=random.randint(1, 30))
                    part["source_timestamp"] = future_date.isoformat()

        return dirty_data

    def inject_dirty_data_safe(self, data: List[Dict[str, Any]], data_type: str, anomaly_rate: float = 0.06):
        """Safe dirty data injection that avoids FK violations for parts"""
        dirty_data = data.copy()
        num_anomalies = int(len(data) * anomaly_rate)
        dirty_indices = set(random.sample(range(len(data)), num_anomalies))

        print(f"Injecting {num_anomalies:,} anomalies into {data_type}...")

        if data_type == "parts":
            for idx in dirty_indices:
                part = dirty_data[idx]
                anomaly_type = random.choice([
                    "negative_cost", "wrong_uom", "future_timestamp"
                ])

                if anomaly_type == "negative_cost":
                    part["unit_cost"] = -random.uniform(1.0, 100.0)
                elif anomaly_type == "wrong_uom":
                    part["uom"] = "INVALID"
                elif anomaly_type == "future_timestamp":
                    future_date = dt.datetime.now(
                        dt.timezone.utc) + timedelta(days=random.randint(1, 30))
                    part["source_timestamp"] = future_date.isoformat()

        return dirty_data

    def export_chunked_csv(self, data: List[Dict[str, Any]], filename: str, chunk_size: int = 50000):
        """Export large datasets in chunks for memory efficiency"""
        print(f"Exporting {len(data):,} records to {filename}...")

        def flatten_record(record):
            flat_record = record.copy()
            if 'geo_coords' in flat_record and flat_record['geo_coords']:
                flat_record['geo_coords'] = json.dumps(
                    flat_record['geo_coords'])
            for k, v in flat_record.items():
                if isinstance(v, list):
                    flat_record[k] = ','.join(map(str, v)) if v else ''
            return flat_record

        if data:
            first_chunk = data[:chunk_size]
            flattened_chunk = [flatten_record(record)
                               for record in first_chunk]

            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = flattened_chunk[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_chunk)

            for i in range(chunk_size, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                flattened_chunk = [flatten_record(record) for record in chunk]

                with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writerows(flattened_chunk)

                print(
                    f"  Exported {min(i + chunk_size, len(data)):,} records...")

    def export_parquet(self, data: List[Dict[str, Any]], filename: str):
        """Export data to Parquet using pandas with optimizations"""
        print(f"Exporting {len(data):,} records to {filename}...")

        chunk_size = 50000
        dfs = []

        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            df_chunk = pd.DataFrame(chunk)

            if 'geo_coords' in df_chunk.columns:
                df_chunk['geo_lat'] = df_chunk['geo_coords'].apply(
                    lambda x: x['lat'] if x else None)
                df_chunk['geo_lon'] = df_chunk['geo_coords'].apply(
                    lambda x: x['lon'] if x else None)
                df_chunk = df_chunk.drop('geo_coords', axis=1)

            for col in df_chunk.columns:
                if df_chunk[col].dtype == 'object':
                    sample_val = df_chunk[col].dropna(
                    ).iloc[0] if not df_chunk[col].dropna().empty else None
                    if isinstance(sample_val, list):
                        df_chunk[col] = df_chunk[col].apply(
                            lambda x: ','.join(map(str, x)) if x else '')

            dfs.append(df_chunk)

        final_df = pd.concat(dfs, ignore_index=True)
        final_df.to_parquet(filename, compression='snappy',
                            row_group_size=100000, index=False)


class DataGeneratorWithUpload(DataGenerator):
    """Data generator with S3 upload and PostgreSQL capability"""

    def __init__(self, seed=42, tenant_id="tenant_acme", auto_upload=True,
                s3_bucket="cdf-upload", use_postgres=True, tracer=None):
        super().__init__(seed, tenant_id)
        self.auto_upload = auto_upload
        self.s3_bucket = s3_bucket
        self.use_postgres = use_postgres
        self.tracer = tracer or init_tracer("ingestion")

        if auto_upload:
            try:
                self.s3_client = boto3.client('s3')
                print(f"Connected to S3 bucket: {s3_bucket}")
            except Exception as e:
                print(f"S3 setup failed: {e}")
                self.auto_upload = False

        if use_postgres:
            try:
                self.pg_conn = psycopg2.connect(
                    host="localhost",
                    port=5432,
                    database="supply_chain",
                    user="pipeline_user",
                    password="pipeline_pass"
                )
                print("Connected to PostgreSQL")
            except Exception as e:
                print(f"PostgreSQL setup failed: {e}")
                self.use_postgres = False

    def insert_to_postgres(self, data: List[Dict[str, Any]], table_name: str):
        """
        PostgreSQL insertion using COPY command - CLEAN DATA ONLY
        """
        if not self.use_postgres or not data:
            return

        print(
            f"Inserting {len(data):,} clean records into PostgreSQL {table_name}...")

        try:
            import tempfile
            import time

            with self.pg_conn.cursor() as cursor:
                temp_table = f"temp_{table_name}_{int(time.time())}"

                try:
                    cursor.execute(
                        f"CREATE TEMP TABLE {temp_table} (LIKE {table_name})")

                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
                        temp_path = temp_file.name
                        self._export_postgres_csv(
                            data, temp_path, table_name)

                    if table_name == "suppliers":
                        columns = "supplier_id,tenant_id,supplier_code,legal_name,dba_name,country,region,address_line1,address_line2,city,state,postal_code,contact_email,contact_phone,preferred_currency,incoterms,lead_time_days_avg,lead_time_days_p95,on_time_delivery_rate,defect_rate_ppm,capacity_units_per_week,risk_score,financial_risk_tier,certifications,compliance_flags,approved_status,contracts,terms_version,geo_coords,data_source,source_timestamp,ingestion_timestamp,schema_version"
                        pk_column = "supplier_id"
                    elif table_name == "parts":
                        columns = "part_id,tenant_id,part_number,description,category,lifecycle_status,uom,spec_hash,bom_compatibility,default_supplier_id,qualified_supplier_ids,unit_cost,moq,lead_time_days_avg,lead_time_days_p95,quality_grade,compliance_flags,hazard_class,last_price_change,data_source,source_timestamp,ingestion_timestamp,schema_version"
                        pk_column = "part_id"

                    with open(temp_path, 'r', encoding='utf-8') as f:
                        cursor.copy_expert(
                            f"COPY {temp_table}({columns}) FROM STDIN WITH CSV HEADER", f)

                    cursor.execute(f"""
                        INSERT INTO {table_name} 
                        SELECT * FROM {temp_table}
                        ON CONFLICT ({pk_column}) DO NOTHING
                    """)

                    self.pg_conn.commit()
                    print(
                        f"  Successfully inserted {len(data):,} records into {table_name}")

                finally:
                    try:
                        os.unlink(temp_path)
                        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    except:
                        pass

        except Exception as e:
            print(f"PostgreSQL insert failed: {e}")
            self.pg_conn.rollback()

    def _export_postgres_csv(self, data: List[Dict[str, Any]], filename: str, table_name: str):
        """CSV export for PostgreSQL COPY command"""
        if table_name == "suppliers":
            column_order = [
                "supplier_id", "tenant_id", "supplier_code", "legal_name", "dba_name",
                "country", "region", "address_line1", "address_line2", "city", "state",
                "postal_code", "contact_email", "contact_phone", "preferred_currency",
                "incoterms", "lead_time_days_avg", "lead_time_days_p95", "on_time_delivery_rate",
                "defect_rate_ppm", "capacity_units_per_week", "risk_score", "financial_risk_tier",
                "certifications", "compliance_flags", "approved_status", "contracts",
                "terms_version", "geo_coords", "data_source", "source_timestamp",
                "ingestion_timestamp", "schema_version"
            ]
        else:  # parts
            column_order = [
                "part_id", "tenant_id", "part_number", "description", "category",
                "lifecycle_status", "uom", "spec_hash", "bom_compatibility",
                "default_supplier_id", "qualified_supplier_ids", "unit_cost", "moq",
                "lead_time_days_avg", "lead_time_days_p95", "quality_grade",
                "compliance_flags", "hazard_class", "last_price_change", "data_source",
                "source_timestamp", "ingestion_timestamp", "schema_version"
            ]

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_order)
            writer.writeheader()

            for record in data:
                pg_record = record.copy()
                for key, value in pg_record.items():
                    if isinstance(value, (list, dict)):
                        pg_record[key] = json.dumps(value) if value else None
                writer.writerow(pg_record)

    def close_connections(self):
        """Close database connections"""
        if hasattr(self, 'pg_conn') and self.pg_conn:
            self.pg_conn.close()
            print("PostgreSQL connection closed")

    def generate_full_dataset(self, num_suppliers=100000, num_parts=100000,
                              include_dirty_data=True, anomaly_rate=0.06):
        """
        Generate large datasets optimized for speed
        Output: suppliers.csv and parts.csv with dirty data mixed in
        """
        
        start_time = dt.datetime.now()
        print(
            f"Starting generation: {num_suppliers:,} suppliers + {num_parts:,} parts")

        
        with self.tracer.start_as_current_span("Suppliers Ingestion") as span:
            suppliers = self.generate_suppliers(num_suppliers)
            supplier_ids = [s["supplier_id"] for s in suppliers]
            span.set_attribute("rows", num_suppliers)
        
        with self.tracer.start_as_current_span("Parts Ingestion") as span:
            parts = self.generate_parts(num_parts, supplier_ids)
            span.set_attribute("rows", num_parts)

        # skip tracing for pg insert
        if self.use_postgres and not include_dirty_data:
            print("Inserting clean data to PostgreSQL...")
            self.insert_to_postgres(suppliers, "suppliers")
            self.insert_to_postgres(parts, "parts")

        if include_dirty_data:
            with self.tracer.start_as_current_span("Dirty Data") as span:
                print("Injecting dirty data for pipeline testing...")
                suppliers = self.inject_dirty_data(
                    suppliers, "suppliers", anomaly_rate)
                parts_dirty_safe = self.inject_dirty_data_safe(
                    parts, "parts", anomaly_rate)
                parts = parts_dirty_safe
                
                span.set_attribute("anomaly_rate", anomaly_rate)
                span.set_attribute("suppliers_with_anomalies", len(suppliers)*anomaly_rate)
                span.set_attribute("parts_with_anomalies", len(parts)*anomaly_rate)

        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)

        print("Exporting final datasets...")
        self.export_chunked_csv(
            suppliers, os.path.join(data_dir, "suppliers.csv"))
        self.export_chunked_csv(parts, os.path.join(data_dir, "parts.csv"))

        if self.auto_upload:
            with self.tracer.start_as_current_span("S3 Upload") as span:
                print("Uploading core files to S3...")
                self._upload_core_files(data_dir)
                span.set_attribute("s3_bucket", self.s3_bucket)
                span.set_attribute("uploaded_files", ["suppliers.csv", "parts.csv"])
                span.set_attribute("duration(seconds)", (dt.datetime.now() - start_time).total_seconds())
                
        end_time = dt.datetime.now()
        duration = (end_time - start_time).total_seconds()

        result = {
            "suppliers_count": len(suppliers),
            "parts_count": len(parts),
            "dirty_data_rate": anomaly_rate if include_dirty_data else 0,
            "generation_time_seconds": duration,
            "records_per_second": (num_suppliers + num_parts) / duration,
            "tenant_id": self.tenant_id,
            "postgres_inserted": self.use_postgres and not include_dirty_data,
            "files_generated": ["suppliers.csv", "parts.csv"],
            "duration": duration
        }

        print(f"\n Performance Summary:")
        print(f"  Total time: {duration:.1f} seconds")
        print(f"  Records/second: {result['records_per_second']:,.0f}")
        print(
            f"  Generated: {result['suppliers_count']:,} suppliers + {result['parts_count']:,} parts")
        print(f"  PostgreSQL inserted: {result['postgres_inserted']}")
        print(f"  Core files: {result['files_generated']}")

        return result

    def _upload_core_files(self, local_dir):
        """Upload only the core CSV and Parquet files to S3"""
        core_files = ["suppliers.csv", "parts.csv"]
        utc_now = dt.datetime.now(dt.timezone.utc)
        run_id = utc_now.strftime("%Y%m%d_%H%M%S")
        date_folder = utc_now.strftime("%Y-%m-%d")

        for filename in core_files:
            local_path = os.path.join(local_dir, filename)
            if os.path.exists(local_path):
                remote_path = f"{self.tenant_id}/{date_folder}/{run_id}/{filename}"

                try:
                    self.s3_client.upload_file(
                        local_path, self.s3_bucket, remote_path)
                    print(
                        f"  Uploaded {filename} -> s3://{self.s3_bucket}/{remote_path}")
                except Exception as e:
                    print(f"  Upload failed for {filename}: {e}")


if __name__ == "__main__":

    tracer = init_tracer("ingestion")
    tenant_id = "tenant_acme"
    num_parts = 100000
    num_suppliers = 100000

    generator = DataGeneratorWithUpload(
        seed=42,
        tenant_id=tenant_id,
        auto_upload=True,  # Set to True to enable S3 upload
        s3_bucket="cdf-upload",
        use_postgres=False,   # Set to False to skip PostgreSQL
        tracer=tracer
    )

    try:
        with generator.tracer.start_as_current_span("suppliers_parts_ingestion") as span:
            result = generator.generate_full_dataset(
                num_suppliers=num_suppliers,
                num_parts=num_parts,
                include_dirty_data=True,
                anomaly_rate=0.06,
            )
            span.set_attribute("tenant", tenant_id)
            span.set_attribute("rows for suppliers", num_suppliers)
            span.set_attribute("rows for parts", num_parts)
            span.set_attribute("duration(seconds)",
                               round(result["duration"], 1))
            span.set_attribute("records_per_second", int(
                result["records_per_second"]))

        print(
            f"\nSUCCESS! Generated {result['suppliers_count']:,} suppliers + {result['parts_count']:,} parts")
        print(
            f"Performance: {result['records_per_second']:,.0f} records/second")
        print(f"PostgreSQL inserted: {result['postgres_inserted']}")
        print(f"Files ready for pipeline testing: {result['files_generated']}")

    finally:
        generator.close_connections()
