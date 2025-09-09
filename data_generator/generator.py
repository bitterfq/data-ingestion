"""
Data Generator for Supply Chain POC

Generates realistic supplier and part test data with controlled correlations
and dirty data injection for testing data pipeline quality gates.

Based on requirements from POC document sections 3.1-3.4.
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


class Generator:
    """
    Generates realistic supplier and part data for testing ingestion pipeline.
    
    Features:
    - Deterministic generation with fixed seed
    - Realistic business correlations (on-time rate vs risk score)
    - Geographic distribution skew
    - Controlled dirty data injection (5-8% anomalies)
    - Referential integrity between suppliers and parts
    """

    def __init__(self, seed: int = 42, tenant_id: str = "tenant_acme"):
        """
        Initialize generator with deterministic seed.
        
        Args:
            seed: Random seed for reproducible data generation
            tenant_id: Tenant identifier for multi-tenant setup
        """
        self.seed = seed
        self.tenant_id = tenant_id

        # Initialize random generators with fixed seed
        random.seed(seed)
        np.random.seed(seed)
        self.fake = Faker()
        Faker.seed(seed)

        # Business assumptions
        self._init_assumptions()

    def _init_assumptions(self):
        """Initialize business correlation assumptions"""

        # Country distribution (Zipfian skew as specified in doc)
        self.country_weights = {
            "CN": 22, "US": 18, "DE": 10, "MX": 10, "IN": 10,
            "VN": 8, "PL": 6, "JP": 8, "KR": 8
        }

        # Certification probabilities (from doc: ISO9001=0.7, IATF16949=0.3, AS9100=0.1)
        self.certifications = ["ISO9001", "IATF16949", "AS9100", "ISO14001"]
        self.cert_probabilities = [0.7, 0.3, 0.1, 0.2]  # Can have multiple

        # Financial risk tier mapping (our assumption based on doc's risk clusters)
        self.risk_tier_thresholds = {
            "LOW": (0, 35),      # Safe cluster 20-40
            "MEDIUM": (35, 60),  # Middle range
            "HIGH": (60, 100)    # Risky cluster 60-85
        }

        # Part category distribution (from doc section 3.2)
        self.part_categories = ["ELECTRICAL",
                                "MECHANICAL", "RAW_MATERIAL", "OTHER"]
        self.category_weights = [0.35, 0.35, 0.20, 0.10]

        # Lead time assumptions by category (our assumption for correlation)
        self.category_lead_times = {
            "ELECTRICAL": (32.0, 8.0),      # μ=32, σ=8 (longest per doc)
            "MECHANICAL": (24.0, 6.0),      # μ=24, σ=6
            "RAW_MATERIAL": (14.0, 4.0),    # μ=14, σ=4 (shortest per doc)
            "OTHER": (20.0, 5.0)            # μ=20, σ=5
        }

    def _calculate_risk_score(self, on_time_rate: float) -> float:
        """Calculate risk score inversely correlated with on-time delivery rate."""
        # Base risk inversely related to on-time performance
        base_risk = max(0, 100 - on_time_rate)
        # Add controlled noise to create bimodal clusters
        noise = np.random.normal(0, 8)
        risk_score = np.clip(base_risk + noise, 0, 100)
        return round(risk_score, 2)

    def _determine_financial_tier(self, risk_score: float) -> str:
        """Map risk score to financial risk tier"""
        for tier, (min_risk, max_risk) in self.risk_tier_thresholds.items():
            if min_risk <= risk_score < max_risk:
                return tier
        return "HIGH"  # Default for scores >= 60

    def _generate_supplier_record(self) -> Dict[str, Any]:
        """Generate single realistic supplier record."""
        supplier_id = ulid.new().str

        # Generate correlated performance metrics
        on_time_rate = np.clip(np.random.normal(93, 5), 60, 100)
        risk_score = self._calculate_risk_score(on_time_rate)
        financial_tier = self._determine_financial_tier(risk_score)

        # Geographic distribution (Zipfian skew)
        country = np.random.choice(
            list(self.country_weights.keys()),
            p=np.array(list(self.country_weights.values())) /
            sum(self.country_weights.values())
        )

        # Lead times with realistic ranges
        lead_time_avg = int(np.clip(np.random.normal(21, 6), 3, 90))
        lead_time_p95 = int(np.clip(np.random.normal(35, 10), 7, 180))

        # Certifications (weighted sampling, can have multiple)
        num_certs = np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        certifications = list(np.random.choice(
            self.certifications,
            size=min(num_certs, len(self.certifications)),
            replace=False,
            p=np.array(self.cert_probabilities) / sum(self.cert_probabilities)
        ))

        # Approval status distribution
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
        """Map country to business region"""
        region_map = {
            "US": "AMERICAS", "MX": "AMERICAS",
            "CN": "APAC", "JP": "APAC", "KR": "APAC", "IN": "APAC", "VN": "APAC",
            "DE": "EMEA", "PL": "EMEA"
        }
        return region_map.get(country, "OTHER")

    def _get_currency(self, country: str) -> str:
        """Map country to preferred currency (ISO-4217)"""
        currency_map = {
            "US": "USD", "CN": "CNY", "DE": "EUR", "MX": "MXN",
            "IN": "INR", "VN": "VND", "PL": "PLN", "JP": "JPY", "KR": "KRW"
        }
        return currency_map.get(country, "USD")

    def _generate_compliance_flags(self) -> List[str]:
        """Generate realistic compliance flags"""
        flags = ["ITAR", "REACH", "ROHS"]
        num_flags = np.random.choice([0, 1, 2], p=[0.4, 0.5, 0.1])
        return list(np.random.choice(flags, size=num_flags, replace=False)) if num_flags > 0 else []

    def _generate_source_timestamp(self) -> str:
        """Generate realistic source timestamp (recent past)"""
        days_ago = random.randint(0, 30)  # Within last 30 days
        timestamp = datetime.now() - timedelta(days=days_ago)
        return timestamp.isoformat()

    def _generate_part_record(self, supplier_ids: List[str]) -> Dict[str, Any]:
        """Generate single realistic part record with supplier references."""
        part_id = ulid.new().str

        # Category distribution
        category = np.random.choice(
            self.part_categories, p=self.category_weights)

        # Lead times by category
        lead_mean, lead_std = self.category_lead_times[category]
        lead_time_avg = int(
            np.clip(np.random.normal(lead_mean, lead_std), 2, 200))
        lead_time_p95 = int(
            np.clip(np.random.normal(lead_mean + 8, lead_std), 5, 250))

        # Unit cost: log-normal distribution
        unit_cost = float(np.exp(np.random.normal(3.0, 0.8)))
        unit_cost = round(unit_cost, 2)

        # Qualified suppliers (1-3 suppliers per part for referential integrity)
        num_qualified = int(np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1]))
        if len(supplier_ids) < num_qualified:
            num_qualified = len(supplier_ids)

        qualified_supplier_ids = list(np.random.choice(
            supplier_ids,
            size=num_qualified,
            replace=False
        ))
        default_supplier_id = qualified_supplier_ids[0]

        # Other attributes
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
            "compliance_flags": self._generate_part_compliance_flags(),
            "hazard_class": self._generate_hazard_class() if random.random() < 0.1 else None,
            "last_price_change": self._generate_price_change_date(),
            "data_source": "synthetic.v1",
            "source_timestamp": self._generate_source_timestamp(),
            "ingestion_timestamp": "2024-01-01T00:00:00",
            "schema_version": "1.0.0"
        }

    def _generate_part_compliance_flags(self) -> List[str]:
        """Generate realistic part compliance flags"""
        flags = ["ROHS", "REACH", "ITAR"]
        num_flags = np.random.choice([0, 1, 2], p=[0.3, 0.6, 0.1])
        return list(np.random.choice(flags, size=num_flags, replace=False)) if num_flags > 0 else []

    def _generate_hazard_class(self) -> str:
        """Generate hazard class for hazardous materials"""
        return np.random.choice(["CLASS_1", "CLASS_3", "CLASS_8", "CLASS_9"])

    def _generate_price_change_date(self) -> str:
        """Generate last price change date (recent past)"""
        days_ago = random.randint(30, 365)
        date = datetime.now() - timedelta(days=days_ago)
        return date.date().isoformat()

    def generate_suppliers(self, count: int = 30000) -> List[Dict[str, Any]]:
        """Generate specified number of supplier records."""
        print(f"Generating {count} supplier records with seed={self.seed}...")

        suppliers = []
        for i in range(count):
            if i % 5000 == 0:
                print(f"  Generated {i}/{count} suppliers...")
            supplier = self._generate_supplier_record()
            suppliers.append(supplier)

        print(f"Generated {len(suppliers)} suppliers")
        return suppliers

    def generate_parts(self, count: int = 30000, supplier_ids: List[str] = None) -> List[Dict[str, Any]]:
        """Generate specified number of part records with supplier references."""
        if not supplier_ids:
            raise ValueError("supplier_ids required for referential integrity")
        if len(supplier_ids) < 1:
            raise ValueError("At least one supplier_id required")

        print(f"Generating {count} part records with seed={self.seed}...")
        print(f"Referencing {len(supplier_ids)} suppliers for FK integrity...")

        parts = []
        for i in range(count):
            if i % 5000 == 0:
                print(f"  Generated {i}/{count} parts...")
            part = self._generate_part_record(supplier_ids)
            parts.append(part)

        print(f"Generated {len(parts)} parts")
        return parts

    def inject_dirty_data(self, data: List[Dict[str, Any]], data_type: str = "suppliers", anomaly_rate: float = 0.06) -> List[Dict[str, Any]]:
        """Inject controlled anomalies for testing data quality gates."""
        dirty_data = data.copy()
        num_anomalies = int(len(data) * anomaly_rate)

        print(
            f"Injecting {num_anomalies} anomalies ({anomaly_rate:.1%} of {data_type})...")

        # Select random records for corruption
        dirty_indices = random.sample(range(len(data)), num_anomalies)

        if data_type == "suppliers":
            self._inject_supplier_anomalies(dirty_data, dirty_indices)
        elif data_type == "parts":
            self._inject_part_anomalies(dirty_data, dirty_indices)
        else:
            raise ValueError(f"Unknown data_type: {data_type}")

        print(f"Injected anomalies in {len(dirty_indices)} records")
        return dirty_data

    def _inject_supplier_anomalies(self, suppliers: List[Dict[str, Any]], dirty_indices: List[int]):
        """Inject supplier-specific anomalies per doc section 3.3"""
        for idx in dirty_indices:
            supplier = suppliers[idx]
            anomaly_type = random.choice([
                "out_of_range_rate", "missing_contact", "bogus_country",
                "duplicate_code", "future_timestamp"
            ])

            if anomaly_type == "out_of_range_rate":
                supplier["on_time_delivery_rate"] = 104.0  # Invalid rate
            elif anomaly_type == "missing_contact":
                supplier["contact_email"] = None
                supplier["contact_phone"] = None
            elif anomaly_type == "bogus_country":
                supplier["country"] = "XX"  # Invalid ISO code
            elif anomaly_type == "duplicate_code":
                if len(suppliers) > 1:
                    other_supplier = random.choice(
                        [s for s in suppliers if s != supplier])
                    supplier["supplier_code"] = other_supplier["supplier_code"]
            elif anomaly_type == "future_timestamp":
                future_date = datetime.now() + timedelta(days=random.randint(1, 30))
                supplier["source_timestamp"] = future_date.isoformat()

    def _inject_part_anomalies(self, parts: List[Dict[str, Any]], dirty_indices: List[int]):
        """Inject part-specific anomalies per doc section 3.3"""
        for idx in dirty_indices:
            part = parts[idx]
            anomaly_type = random.choice([
                "invalid_supplier_id", "negative_cost", "wrong_uom",
                "duplicate_part_number", "future_timestamp"
            ])

            if anomaly_type == "invalid_supplier_id":
                # Create orphan FK by using non-existent supplier ID
                part["qualified_supplier_ids"] = [ulid.new().str]
                part["default_supplier_id"] = part["qualified_supplier_ids"][0]
            elif anomaly_type == "negative_cost":
                part["unit_cost"] = -random.uniform(1.0, 100.0)
            elif anomaly_type == "wrong_uom":
                part["uom"] = "INVALID_UOM"  # Not in standard set
            elif anomaly_type == "duplicate_part_number":
                if len(parts) > 1:
                    other_part = random.choice([p for p in parts if p != part])
                    part["part_number"] = other_part["part_number"]
            elif anomaly_type == "future_timestamp":
                future_date = datetime.now() + timedelta(days=random.randint(1, 30))
                part["source_timestamp"] = future_date.isoformat()

    def export_to_parquet(self, data: List[Dict[str, Any]], filename: str):
        """Export generated data to Parquet file with proper schema and compression."""
        if not data:
            print("No data to export")
            return

        print(f"Exporting {len(data)} records to {filename}...")

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Handle nested structures properly for Parquet
        if 'geo_coords' in df.columns:
            df['geo_lat'] = df['geo_coords'].apply(
                lambda x: x['lat'] if x else None)
            df['geo_lon'] = df['geo_coords'].apply(
                lambda x: x['lon'] if x else None)
            df = df.drop('geo_coords', axis=1)

        # Convert arrays to strings (Parquet doesn't handle mixed arrays well)
        for col in df.columns:
            if df[col].dtype == 'object':
                sample_val = df[col].dropna(
                ).iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample_val, list):
                    df[col] = df[col].apply(
                        lambda x: ','.join(map(str, x)) if x else '')

        # Export with compression and proper row group size
        df.to_parquet(filename, compression='snappy',
                      row_group_size=50000, index=False)
        print(f"Exported to {filename}")

    def export_to_csv(self, data: List[Dict[str, Any]], filename: str):
        """Export generated data to CSV file."""
        if not data:
            print("No data to export")
            return

        print(f"Exporting {len(data)} records to {filename}...")

        # Flatten nested structures
        flattened_data = []
        for record in data:
            flat_record = record.copy()

            # Handle geo_coords
            if 'geo_coords' in flat_record and flat_record['geo_coords']:
                flat_record['geo_lat'] = flat_record['geo_coords']['lat']
                flat_record['geo_lon'] = flat_record['geo_coords']['lon']
                del flat_record['geo_coords']

            # Convert arrays to string representation
            for key, value in flat_record.items():
                if isinstance(value, list):
                    flat_record[key] = ','.join(
                        map(str, value)) if value else ''

            flattened_data.append(flat_record)

        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if flattened_data:
                fieldnames = flattened_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_data)

        print(f"Exported to {filename}")


class GeneratorWithUpload(Generator):
    """Extends Generator to automatically upload files to S3 after generation"""

    def __init__(self, seed=42, tenant_id="tenant_acme", auto_upload=True,
                 s3_bucket="cdf-upload"):
        super().__init__(seed, tenant_id)

        self.auto_upload = auto_upload
        self.s3_bucket = s3_bucket

        if auto_upload:
            try:
                # Use AWS S3 instead of MinIO
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    region_name=os.getenv("AWS_REGION")
                )

                # Verify bucket exists
                try:
                    self.s3_client.head_bucket(Bucket=s3_bucket)
                    print(f"Connected to S3 bucket: {s3_bucket}")
                except ClientError as e:
                    print(f"S3 bucket access error: {e}")
                    self.auto_upload = False

            except Exception as e:
                print(f"S3 connection failed: {e}")
                print("Continuing without upload capability...")
                self.auto_upload = False

    def export_to_csv(self, data, filename):
        """Override to upload after local export, always write to data/ subdir"""
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)
        out_path = os.path.join(data_dir, os.path.basename(filename))
        super().export_to_csv(data, out_path)
        if self.auto_upload:
            self._upload_file(out_path)

    def _upload_file(self, local_filename):
        """Upload single file to S3 with organized structure"""
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_folder = datetime.now().strftime("%Y-%m-%d")

        # Use the generator's tenant_id (no parameter passing)
        remote_path = f"{self.tenant_id}/{date_folder}/{run_id}/{os.path.basename(local_filename)}"

        try:
            self.s3_client.upload_file(
                local_filename, self.s3_bucket, remote_path)
            print(
                f"Uploaded: {local_filename} -> s3://{self.s3_bucket}/{remote_path}")
            return remote_path
        except Exception as e:
            print(f"Upload failed for {local_filename}: {e}")
            return None

    def generate_and_export_full_dataset(self, num_suppliers=30000, num_parts=30000,
                                         include_dirty_data=True, anomaly_rate=0.06):
        """Complete workflow: generate -> inject dirty data -> export -> upload"""
        print(f"Starting full dataset generation for {self.tenant_id}...")

        # Generate data
        suppliers = self.generate_suppliers(num_suppliers)
        supplier_ids = [s["supplier_id"] for s in suppliers]
        parts = self.generate_parts(num_parts, supplier_ids)

        # Inject dirty data if requested
        if include_dirty_data:
            suppliers = self.inject_dirty_data(
                suppliers, "suppliers", anomaly_rate)
            parts = self.inject_dirty_data(parts, "parts", anomaly_rate)

        # Export (will auto-upload if enabled)
        self.export_to_csv(suppliers, "suppliers.csv")
        self.export_to_csv(parts, "parts.csv")

        # Also create parquet for local analysis (always in data/)
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)

        self.export_to_parquet(suppliers, os.path.join(
            data_dir, "suppliers.parquet"))
        self.export_to_parquet(parts, os.path.join(data_dir, "parts.parquet"))

        print(f"Dataset generation completed for {self.tenant_id}")
        return {
            "suppliers_count": len(suppliers),
            "parts_count": len(parts),
            "dirty_data_rate": anomaly_rate if include_dirty_data else 0,
            "tenant_id": self.tenant_id
        }


if __name__ == "__main__":
    # Create generator with auto-upload enabled
    generator = GeneratorWithUpload(
        seed=42,
        tenant_id="tenant_acme",
        auto_upload=True  # Set to False to disable S3 upload
    )

    # Generate everything - files automatically uploaded
    result = generator.generate_and_export_full_dataset(
        num_suppliers=45000,
        num_parts=45000,
        include_dirty_data=True
    )

    print(
        f"Complete! Generated {result['suppliers_count']} suppliers, {result['parts_count']} parts")
