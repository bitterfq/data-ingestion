"""
Data Generator for Supply Chain POC

Generates realistic supplier and part test data with controlled correlations
and dirty data injection for testing data pipeline quality gates.

Based on requirements from POC document sections 3.1-3.4.

Features:
- Deterministic generation with fixed seed
- Realistic business correlations (on-time rate vs risk score)
- Geographic distribution skew
- Controlled dirty data injection (5-8% anomalies)
- Referential integrity between suppliers and parts
- Optional S3 upload of generated files
- PostgreSQL database insertion
"""

import os
import random
import numpy as np
import pandas as pd
from faker import Faker
from typing import List, Dict, Any, Optional
import ulid
from datetime import datetime, timedelta
import csv
import boto3
from botocore.exceptions import ClientError
import psycopg2
import psycopg2.extras
import json
import decimal
import csv


# --- FIX: canonical parts headers to keep schema stable ---
PARTS_HEADERS = [
    "part_id", "tenant_id", "part_number", "description", "category", "lifecycle_status",
    "uom", "spec_hash", "bom_compatibility", "default_supplier_id", "qualified_supplier_ids",
    "unit_cost", "moq", "lead_time_days_avg", "lead_time_days_p95", "quality_grade",
    "compliance_flags", "hazard_class", "last_price_change", "data_source",
    "source_timestamp", "ingestion_timestamp", "schema_version"
]


class Generator:
    """
    Generates realistic supplier and part data for testing ingestion pipeline.

    Methods:
        generate_suppliers(count): Generate a list of supplier records.
        generate_parts(count, supplier_ids): Generate a list of part records with referential integrity.
        inject_dirty_data(data, data_type, anomaly_rate): Inject dirty data for testing.
        export_to_parquet(data, filename): Export data to a Parquet file.
        export_to_csv(data, filename): Export data to a CSV file.
    """

    def __init__(self, seed: int = 42, tenant_id: str = "tenant_acme"):
        """
        Initialize the data generator with a random seed and tenant ID.

        Args:
            seed (int): Random seed for reproducibility.
            tenant_id (str): Tenant identifier for generated records.
        """
        self.seed = seed
        self.tenant_id = tenant_id
        random.seed(seed)
        np.random.seed(seed)
        self.fake = Faker()
        Faker.seed(seed)
        self._init_assumptions()

    def _init_assumptions(self):
        """
        Initialize internal assumptions and distributions for data generation.
        """
        self.country_weights = {
            "CN": 22, "US": 18, "DE": 10, "MX": 10, "IN": 10,
            "VN": 8, "PL": 6, "JP": 8, "KR": 8
        }
        self.certifications = ["ISO9001", "IATF16949", "AS9100", "ISO14001"]
        self.cert_probabilities = [0.7, 0.3, 0.1, 0.2]
        self.risk_tier_thresholds = {
            "LOW": (0, 35),
            "MEDIUM": (35, 60),
            "HIGH": (60, 100)
        }
        self.part_categories = ["ELECTRICAL",
                                "MECHANICAL", "RAW_MATERIAL", "OTHER"]
        self.category_weights = [0.35, 0.35, 0.20, 0.10]
        self.category_lead_times = {
            "ELECTRICAL": (32.0, 8.0),
            "MECHANICAL": (24.0, 6.0),
            "RAW_MATERIAL": (14.0, 4.0),
            "OTHER": (20.0, 5.0)
        }

    def _calculate_risk_score(self, on_time_rate: float) -> float:
        """
        Calculate a risk score based on on-time delivery rate.

        Args:
            on_time_rate (float): On-time delivery rate.

        Returns:
            float: Calculated risk score.
        """
        base_risk = max(0, 100 - on_time_rate)
        noise = np.random.normal(0, 8)
        risk_score = np.clip(base_risk + noise, 0, 100)
        return round(risk_score, 2)

    def _determine_financial_tier(self, risk_score: float) -> str:
        """
        Determine the financial risk tier based on risk score.

        Args:
            risk_score (float): Risk score.

        Returns:
            str: Financial risk tier ("LOW", "MEDIUM", "HIGH").
        """
        for tier, (min_risk, max_risk) in self.risk_tier_thresholds.items():
            if min_risk <= risk_score < max_risk:
                return tier
        return "HIGH"

    def _generate_supplier_record(self) -> Dict[str, Any]:
        """
        Generate a single supplier record with realistic fields.

        Returns:
            dict: Supplier record.
        """
        supplier_id = ulid.new().str
        on_time_rate = np.clip(np.random.normal(93, 5), 60, 100)
        risk_score = self._calculate_risk_score(on_time_rate)
        financial_tier = self._determine_financial_tier(risk_score)
        country = np.random.choice(list(self.country_weights.keys()),
                                   p=np.array(list(self.country_weights.values())) /
                                   sum(self.country_weights.values()))
        lead_time_avg = int(np.clip(np.random.normal(21, 6), 3, 90))
        lead_time_p95 = int(np.clip(np.random.normal(35, 10), 7, 180))

        num_certs = np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        certifications = list(np.random.choice(
            self.certifications,
            size=min(num_certs, len(self.certifications)),
            replace=False,
            p=np.array(self.cert_probabilities) / sum(self.cert_probabilities)
        ))

        approved_status = np.random.choice(
            ["APPROVED", "PENDING", "SUSPENDED", "BLACKLISTED"],
            p=[0.80, 0.15, 0.04, 0.01]
        )

        return {
            "supplier_id": supplier_id,
            "supplier_code": f"S{random.randint(100000, 999999)}",
            "tenant_id": self.tenant_id,
            "legal_name": self.fake.company(),
            "dba_name": self.fake.company() if random.random() < 0.3 else None,
            "country": country,
            "region": self._get_region(country),
            "address_line1": self.fake.street_address(),
            "address_line2": self.fake.secondary_address() if random.random() < 0.2 else None,
            "city": self.fake.city(),
            "state": self.fake.state_abbr() if country == "US" else None,
            "postal_code": self.fake.postcode(),
            "contact_email": self.fake.email(),
            "contact_phone": self.fake.phone_number(),
            "preferred_currency": self._get_currency(country),
            "incoterms": np.random.choice(["DDP", "FOB", "CIF", "EXW"], p=[0.4, 0.3, 0.2, 0.1]),
            "lead_time_days_avg": lead_time_avg,
            "lead_time_days_p95": lead_time_p95,
            "on_time_delivery_rate": float(round(on_time_rate, 2)),
            "defect_rate_ppm": int(abs(np.random.normal(250, 180))),
            "capacity_units_per_week": int(abs(np.random.normal(5000, 3000))) + 100,
            "risk_score": float(risk_score),
            "financial_risk_tier": financial_tier,
            "certifications": certifications,
            "compliance_flags": self._generate_compliance_flags(),
            "approved_status": approved_status,
            "contracts": [f"CONTRACT_{ulid.new().str[:8]}" for _ in range(random.randint(0, 2))],
            "terms_version": f"{random.randint(1, 5)}.{random.randint(0, 9)}",
            "geo_coords": {
                "lat": round(self.fake.latitude(), 6),
                "lon": round(self.fake.longitude(), 6)
            },
            "data_source": "synthetic.v1",
            "source_timestamp": self._generate_source_timestamp(),
            "ingestion_timestamp": "2024-01-01T00:00:00",
            "schema_version": "1.0.0"
        }

    def _get_region(self, country: str) -> str:
        """
        Map a country code to a region.

        Args:
            country (str): Country code.

        Returns:
            str: Region name.
        """
        region_map = {
            "US": "AMERICAS", "MX": "AMERICAS",
            "CN": "APAC", "JP": "APAC", "KR": "APAC", "IN": "APAC", "VN": "APAC",
            "DE": "EMEA", "PL": "EMEA"
        }
        return region_map.get(country, "OTHER")

    def _get_currency(self, country: str) -> str:
        """
        Map a country code to a currency.

        Args:
            country (str): Country code.

        Returns:
            str: Currency code.
        """
        currency_map = {
            "US": "USD", "CN": "CNY", "DE": "EUR", "MX": "MXN",
            "IN": "INR", "VN": "VND", "PL": "PLN", "JP": "JPY", "KR": "KRW"
        }
        return currency_map.get(country, "USD")

    def _generate_compliance_flags(self) -> List[str]:
        """
        Randomly generate compliance flags for a supplier or part.

        Returns:
            list: List of compliance flags.
        """
        flags = ["ITAR", "REACH", "ROHS"]
        num_flags = np.random.choice([0, 1, 2], p=[0.4, 0.5, 0.1])
        return list(np.random.choice(flags, size=num_flags, replace=False)) if num_flags > 0 else []

    def _generate_source_timestamp(self) -> str:
        """
        Generate a random ISO timestamp within the last 30 days.

        Returns:
            str: ISO formatted timestamp.
        """
        days_ago = random.randint(0, 30)
        timestamp = datetime.now() - timedelta(days=days_ago)
        return timestamp.isoformat()

    def _generate_part_record(self, supplier_ids: List[str]) -> Dict[str, Any]:
        """
        Generate a single part record with referential integrity to suppliers.

        Args:
            supplier_ids (list): List of valid supplier IDs.

        Returns:
            dict: Part record.
        """
        part_id = ulid.new().str
        category = np.random.choice(
            self.part_categories, p=self.category_weights)
        lead_mean, lead_std = self.category_lead_times[category]
        lead_time_avg = int(
            np.clip(np.random.normal(lead_mean, lead_std), 2, 200))
        lead_time_p95 = int(
            np.clip(np.random.normal(lead_mean + 8, lead_std), 5, 250))
        unit_cost = round(float(np.exp(np.random.normal(3.0, 0.8))), 2)
        num_qualified = int(np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1]))
        if len(supplier_ids) < num_qualified:
            num_qualified = len(supplier_ids)
        qualified_supplier_ids = list(np.random.choice(
            supplier_ids, size=num_qualified, replace=False))
        default_supplier_id = qualified_supplier_ids[0]
        lifecycle_status = str(np.random.choice(
            ["ACTIVE", "NEW", "NRND", "EOL"],
            p=[0.75, 0.10, 0.10, 0.05]
        ))
        quality_grade = str(np.random.choice(
            ["A", "B", "C"], p=[0.6, 0.3, 0.1]))
        moq = int(np.random.choice([1, 10, 50, 100, 500]))
        uom = str(np.random.choice(["EA", "KG", "M"], p=[0.7, 0.2, 0.1]))

        return {
            "part_id": part_id,
            "tenant_id": self.tenant_id,
            "part_number": f"P-{random.randint(100000, 999999)}",
            "description": self.fake.sentence(nb_words=5),
            "category": str(category),
            "lifecycle_status": lifecycle_status,
            "uom": uom,
            "spec_hash": f"SPEC_{ulid.new().str[:16]}",
            "bom_compatibility": [f"BOM_{ulid.new().str[:8]}" for _ in range(random.randint(0, 2))],
            "default_supplier_id": default_supplier_id,
            "qualified_supplier_ids": qualified_supplier_ids,
            "unit_cost": unit_cost,
            "moq": moq,
            "lead_time_days_avg": lead_time_avg,
            "lead_time_days_p95": lead_time_p95,
            "quality_grade": quality_grade,
            "compliance_flags": self._generate_compliance_flags(),
            "hazard_class": self._generate_hazard_class() if random.random() < 0.1 else None,
            "last_price_change": self._generate_price_change_date(),
            "data_source": "synthetic.v1",
            "source_timestamp": self._generate_source_timestamp(),
            "ingestion_timestamp": "2024-01-01T00:00:00",
            "schema_version": "1.0.0"
        }

    def _generate_part_compliance_flags(self) -> List[str]:
        """
        Randomly generate compliance flags for a part.

        Returns:
            list: List of compliance flags.
        """
        flags = ["ROHS", "REACH", "ITAR"]
        num_flags = np.random.choice([0, 1, 2], p=[0.3, 0.6, 0.1])
        return list(np.random.choice(flags, size=num_flags, replace=False)) if num_flags > 0 else []

    def _generate_hazard_class(self) -> str:
        """
        Randomly select a hazard class for a part.

        Returns:
            str: Hazard class.
        """
        return np.random.choice(["CLASS_1", "CLASS_3", "CLASS_8", "CLASS_9"])

    def _generate_price_change_date(self) -> str:
        """
        Generate a random date for last price change.

        Returns:
            str: ISO formatted date.
        """
        days_ago = random.randint(30, 365)
        date = datetime.now() - timedelta(days=days_ago)
        return date.date().isoformat()

    def generate_suppliers(self, count: int = 30000) -> List[Dict[str, Any]]:
        """
        Generate a list of supplier records.

        Args:
            count (int): Number of suppliers to generate.

        Returns:
            list: List of supplier records.
        """
        suppliers = []
        for i in range(count):
            suppliers.append(self._generate_supplier_record())
        return suppliers

    def generate_parts(self, count: int = 30000, supplier_ids: List[str] = None) -> List[Dict[str, Any]]:
        """
        Generate a list of part records with referential integrity to suppliers.

        Args:
            count (int): Number of parts to generate.
            supplier_ids (list): List of valid supplier IDs.

        Returns:
            list: List of part records.
        """
        if not supplier_ids:
            raise ValueError("supplier_ids required for referential integrity")
        parts = []
        for i in range(count):
            parts.append(self._generate_part_record(supplier_ids))
        return parts

    def inject_dirty_data(self, data: List[Dict[str, Any]], data_type: str = "suppliers", anomaly_rate: float = 0.06) -> List[Dict[str, Any]]:
        """
        Inject dirty data (anomalies) into the dataset for negative testing.

        Args:
            data (list): List of records to inject anomalies into.
            data_type (str): Type of data ("suppliers" or "parts").
            anomaly_rate (float): Fraction of records to corrupt.

        Returns:
            list: Data with injected anomalies.
        """
        dirty_data = data.copy()
        num_anomalies = int(len(data) * anomaly_rate)
        dirty_indices = random.sample(range(len(data)), num_anomalies)
        if data_type == "suppliers":
            self._inject_supplier_anomalies(dirty_data, dirty_indices)
        elif data_type == "parts":
            self._inject_part_anomalies(dirty_data, dirty_indices)
        else:
            raise ValueError(f"Unknown data_type: {data_type}")
        return dirty_data

    def _inject_supplier_anomalies(self, suppliers: List[Dict[str, Any]], dirty_indices: List[int]):
        """
        Inject anomalies into supplier records at specified indices.

        Args:
            suppliers (list): List of supplier records.
            dirty_indices (list): Indices to inject anomalies.
        """
        for idx in dirty_indices:
            supplier = suppliers[idx]
            anomaly_type = random.choice([
                "out_of_range_rate", "missing_contact", "bogus_country",
                "duplicate_code", "future_timestamp"
            ])
            if anomaly_type == "out_of_range_rate":
                supplier["on_time_delivery_rate"] = 104.0
            elif anomaly_type == "missing_contact":
                supplier["contact_email"] = None
                supplier["contact_phone"] = None
            elif anomaly_type == "bogus_country":
                supplier["country"] = "XX"
            elif anomaly_type == "duplicate_code":
                if len(suppliers) > 1:
                    other_supplier = random.choice(
                        [s for s in suppliers if s != supplier])
                    supplier["supplier_code"] = other_supplier["supplier_code"]
            elif anomaly_type == "future_timestamp":
                future_date = datetime.now() + timedelta(days=random.randint(1, 30))
                supplier["source_timestamp"] = future_date.isoformat()

    def _inject_part_anomalies(self, parts: List[Dict[str, Any]], dirty_indices: List[int]):
        """
        Inject anomalies into part records at specified indices.

        Args:
            parts (list): List of part records.
            dirty_indices (list): Indices to inject anomalies.
        """
        for idx in dirty_indices:
            part = parts[idx]
            anomaly_type = random.choice([
                "invalid_supplier_id", "negative_cost", "wrong_uom",
                "duplicate_part_number", "future_timestamp"
            ])
            if anomaly_type == "invalid_supplier_id":
                part["qualified_supplier_ids"] = [ulid.new().str]
                part["default_supplier_id"] = part["qualified_supplier_ids"][0]
            elif anomaly_type == "negative_cost":
                part["unit_cost"] = -random.uniform(1.0, 100.0)
            elif anomaly_type == "wrong_uom":
                part["uom"] = "INVALID_UOM"
            elif anomaly_type == "duplicate_part_number":
                if len(parts) > 1:
                    other_part = random.choice([p for p in parts if p != part])
                    part["part_number"] = other_part["part_number"]
            elif anomaly_type == "future_timestamp":
                future_date = datetime.now() + timedelta(days=random.randint(1, 30))
                part["source_timestamp"] = future_date.isoformat()

    def export_to_parquet(self, data: List[Dict[str, Any]], filename: str):
        """
        Export data to a Parquet file.

        Args:
            data (list): List of records to export.
            filename (str): Output Parquet file path.
        """
        if not data:
            return
        df = pd.DataFrame(data)
        if 'geo_coords' in df.columns:
            df['geo_lat'] = df['geo_coords'].apply(
                lambda x: x['lat'] if x else None)
            df['geo_lon'] = df['geo_coords'].apply(
                lambda x: x['lon'] if x else None)
            df = df.drop('geo_coords', axis=1)
        for col in df.columns:
            if df[col].dtype == 'object':
                sample_val = df[col].dropna(
                ).iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample_val, list):
                    df[col] = df[col].apply(
                        lambda x: ','.join(map(str, x)) if x else '')
        df.to_parquet(filename, compression='snappy',
                      row_group_size=50000, index=False)

    def export_to_csv(self, data: List[Dict[str, Any]], filename: str):
        """
        Export data to a CSV file.

        Args:
            data (list): List of records to export.
            filename (str): Output CSV file path.
        """
        flattened_data = []
        for record in data:
            flat_record = record.copy()
            if 'geo_coords' in flat_record and flat_record['geo_coords']:
                # Convert Decimal objects to float before JSON serialization
                geo_data = {}
                for k, v in flat_record['geo_coords'].items():
                    if isinstance(v, decimal.Decimal):
                        geo_data[k] = float(v)
                    else:
                        geo_data[k] = v
                flat_record['geo_coords'] = json.dumps(geo_data)
            for k, v in flat_record.items():
                if isinstance(v, list):
                    flat_record[k] = ','.join(map(str, v)) if v else ''
            flattened_data.append(flat_record)

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if os.path.basename(filename).lower().startswith("parts"):
                for r in flattened_data:
                    for k in PARTS_HEADERS:
                        r.setdefault(k, "")
                fieldnames = PARTS_HEADERS
            else:
                fieldnames = flattened_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flattened_data)


class GeneratorWithUpload(Generator):
    """
    Extends Generator to support automatic upload of generated files to S3 and PostgreSQL insertion.

    Methods:
        export_to_csv(data, filename): Export data to CSV and upload to S3.
        insert_to_postgres(data, table_name): Insert data into PostgreSQL.
        _upload_file(local_filename): Upload a file to the configured S3 bucket.
        generate_and_export_full_dataset(...): Generate, export, and upload a full dataset.
        close_connections(): Close database connections.
    """

    def __init__(self, seed=42, tenant_id="tenant_acme", auto_upload=True,
                 s3_bucket="cdf-upload", use_postgres=True):
        """
        Initialize the generator with S3 upload and PostgreSQL capability.

        Args:
            seed (int): Random seed for reproducibility.
            tenant_id (str): Tenant identifier.
            auto_upload (bool): Whether to upload files to S3 automatically.
            s3_bucket (str): S3 bucket name for uploads.
            use_postgres (bool): Whether to insert data into PostgreSQL.
        """
        super().__init__(seed, tenant_id)
        self.auto_upload = auto_upload
        self.s3_bucket = s3_bucket
        self.use_postgres = use_postgres

        # S3 setup
        if auto_upload:
            try:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    region_name=os.getenv("AWS_REGION")
                )
                self.s3_client.head_bucket(Bucket=s3_bucket)
                print(f"Connected to S3 bucket: {s3_bucket}")
            except Exception as e:
                print(f"S3 setup failed: {e}")
                self.auto_upload = False

        # PostgreSQL setup
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

    def _prepare_record_for_postgres(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare a record for PostgreSQL insertion by converting lists to JSON.
        
        Args:
            record (dict): Record to prepare.
            
        Returns:
            dict: Prepared record.
        """
        pg_record = record.copy()

        # Convert lists to JSON strings for PostgreSQL JSONB columns
        for key, value in pg_record.items():
            if isinstance(value, list):
                pg_record[key] = json.dumps(value) if value else None
            elif isinstance(value, dict):
                pg_record[key] = json.dumps(value) if value else None

        return pg_record

    def insert_to_postgres(self, data: List[Dict[str, Any]], table_name: str):
        """
        Insert data into PostgreSQL table using COPY command.
        
        Args:
            data (list): List of records to insert.
            table_name (str): Target table name ("suppliers" or "parts").
        """
        if not self.use_postgres or not data:
            return

        try:
            import tempfile
            import os

            with self.pg_conn.cursor() as cursor:
                # Clear existing data for this tenant
                cursor.execute(
                    f"DELETE FROM {table_name} WHERE tenant_id = %s", (self.tenant_id,))

                # Create temporary CSV file with database-compatible format
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
                    temp_path = temp_file.name

                    # Create CSV data that matches database schema exactly
                    self._export_postgres_csv(data, temp_path)

                # Define explicit column order to match PostgreSQL schema
                if table_name == "suppliers":
                    columns = "supplier_id,tenant_id,supplier_code,legal_name,dba_name,country,region,address_line1,address_line2,city,state,postal_code,contact_email,contact_phone,preferred_currency,incoterms,lead_time_days_avg,lead_time_days_p95,on_time_delivery_rate,defect_rate_ppm,capacity_units_per_week,risk_score,financial_risk_tier,certifications,compliance_flags,approved_status,contracts,terms_version,geo_coords,data_source,source_timestamp,ingestion_timestamp,schema_version"
                elif table_name == "parts":
                    columns = "part_id,tenant_id,part_number,description,category,lifecycle_status,uom,spec_hash,bom_compatibility,default_supplier_id,qualified_supplier_ids,unit_cost,moq,lead_time_days_avg,lead_time_days_p95,quality_grade,compliance_flags,hazard_class,last_price_change,data_source,source_timestamp,ingestion_timestamp,schema_version"

                # Use COPY command to bulk insert with explicit column mapping
                with open(temp_path, 'r', encoding='utf-8') as f:
                    cursor.copy_expert(
                        f"COPY {table_name}({columns}) FROM STDIN WITH CSV HEADER", f)

                # Clean up temp file
                os.unlink(temp_path)

                self.pg_conn.commit()
                print(
                    f"Inserted {len(data)} records into {table_name} using COPY")

        except Exception as e:
            print(f"PostgreSQL COPY insert failed: {e}")
            self.pg_conn.rollback()

    def _export_postgres_csv(self, data: List[Dict[str, Any]], filename: str):
        """
        Export data to CSV format that matches PostgreSQL schema exactly.
        
        Args:
            data (list): List of records to export.
            filename (str): Output CSV file path.
        """
        if not data:
            return

        # Define the exact column order for each table
        if 'supplier_id' in data[0]:  # suppliers table
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
        else:  # parts table
            column_order = [
                "part_id", "tenant_id", "part_number", "description", "category",
                "lifecycle_status", "uom", "spec_hash", "bom_compatibility",
                "default_supplier_id", "qualified_supplier_ids", "unit_cost", "moq",
                "lead_time_days_avg", "lead_time_days_p95", "quality_grade",
                "compliance_flags", "hazard_class", "last_price_change", "data_source",
                "source_timestamp", "ingestion_timestamp", "schema_version"
            ]

        prepared_data = []
        for record in data:
            pg_record = record.copy()

            # Handle all problematic types
            for key, value in pg_record.items():
                if isinstance(value, list):
                    pg_record[key] = json.dumps(value) if value else None
                elif isinstance(value, dict):
                    # Convert Decimal objects in nested dicts to float
                    clean_dict = {}
                    for k, v in value.items():
                        if isinstance(v, decimal.Decimal):
                            clean_dict[k] = float(v)
                        else:
                            clean_dict[k] = v
                    pg_record[key] = json.dumps(clean_dict) if clean_dict else None
                elif isinstance(value, decimal.Decimal):
                    pg_record[key] = float(value)
                elif key == 'uom' and value == 'INVALID_UOM':
                    # Fix dirty data that's too long for column
                    pg_record[key] = 'INVALID'

            prepared_data.append(pg_record)

        # Write CSV with exact column order
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_order)
            writer.writeheader()
            writer.writerows(prepared_data)
    def export_to_csv(self, data, filename):
        """
        Export data to CSV and upload to S3 if enabled.

        Args:
            data (list): List of records to export.
            filename (str): Output CSV file name.
        """
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)
        out_path = os.path.join(data_dir, os.path.basename(filename))
        super().export_to_csv(data, out_path)
        if self.auto_upload:
            self._upload_file(out_path)

    def _upload_file(self, local_filename):
        """
        Upload a file to the configured S3 bucket.

        Args:
            local_filename (str): Path to the local file to upload.
        """
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_folder = datetime.now().strftime("%Y-%m-%d")
        remote_path = f"{self.tenant_id}/{date_folder}/{run_id}/{os.path.basename(local_filename)}"
        try:
            self.s3_client.upload_file(
                local_filename, self.s3_bucket, remote_path)
            print(
                f"Uploaded {local_filename} -> s3://{self.s3_bucket}/{remote_path}")
            return remote_path
        except Exception as e:
            print(f"Upload failed: {e}")
            return None

    def close_connections(self):
        """
        Close database connections.
        """
        if hasattr(self, 'pg_conn') and self.pg_conn:
            self.pg_conn.close()
            print("PostgreSQL connection closed")

    def generate_and_export_full_dataset(self, num_suppliers=30000, num_parts=30000,
                                         include_dirty_data=True, anomaly_rate=0.06):
        """
        Generate suppliers and parts, inject dirty data, export to CSV/Parquet, upload to S3, and insert to PostgreSQL.

        Args:
            num_suppliers (int): Number of suppliers to generate.
            num_parts (int): Number of parts to generate.
            include_dirty_data (bool): Whether to inject dirty data.
            anomaly_rate (float): Rate of dirty data injection.

        Returns:
            dict: Summary of generated data.
        """
        # Generate data
        suppliers = self.generate_suppliers(num_suppliers)
        supplier_ids = [s["supplier_id"] for s in suppliers]
        parts = self.generate_parts(num_parts, supplier_ids)

        if include_dirty_data:
            suppliers = self.inject_dirty_data(
                suppliers, "suppliers", anomaly_rate)
            parts = self.inject_dirty_data(parts, "parts", anomaly_rate)

        # Export to CSV/Parquet (existing functionality)
        self.export_to_csv(suppliers, "suppliers.csv")
        self.export_to_csv(parts, "parts.csv")

        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)
        self.export_to_parquet(suppliers, os.path.join(
            data_dir, "suppliers.parquet"))
        self.export_to_parquet(parts, os.path.join(data_dir, "parts.parquet"))

        # Insert to PostgreSQL (new functionality) - SUPPLIERS FIRST, THEN PARTS
        if self.use_postgres:
            self.insert_to_postgres(suppliers, "suppliers")
            self.insert_to_postgres(parts, "parts")

        return {
            "suppliers_count": len(suppliers),
            "parts_count": len(parts),
            "dirty_data_rate": anomaly_rate if include_dirty_data else 0,
            "tenant_id": self.tenant_id,
            "postgres_inserted": self.use_postgres
        }


if __name__ == "__main__":
    # Example usage: generate and export a full dataset
    generator = GeneratorWithUpload(
        seed=42, tenant_id="tenant_dddd", auto_upload=False, use_postgres=True)

    try:
        result = generator.generate_and_export_full_dataset(
            num_suppliers=10000, num_parts=10000, include_dirty_data=False
        )
        print(
            f"Complete! Generated {result['suppliers_count']} suppliers, {result['parts_count']} parts")
        print(f"PostgreSQL inserted: {result['postgres_inserted']}")
    finally:
        generator.close_connections()
