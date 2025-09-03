"""
Data Generator for Supply Chain POC

Generates realistic supplier and part test data with controlled correlations
and dirty data injection for testing data pipeline quality gates.

Based on requirements from POC document sections 3.1-3.4.
"""

import random
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from typing import List, Dict, Any, Optional
import ulid
from datetime import datetime, timedelta
import csv


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
        self.part_categories = ["ELECTRICAL", "MECHANICAL", "RAW_MATERIAL", "OTHER"]
        self.category_weights = [0.35, 0.35, 0.20, 0.10]
        
        # Lead time assumptions by category (our assumption for correlation)
        self.category_lead_times = {
            "ELECTRICAL": (32.0, 8.0),      # μ=32, σ=8 (longest per doc)
            "MECHANICAL": (24.0, 6.0),      # μ=24, σ=6
            "RAW_MATERIAL": (14.0, 4.0),    # μ=14, σ=4 (shortest per doc)
            "OTHER": (20.0, 5.0)            # μ=20, σ=5
        }
    
    def _calculate_risk_score(self, on_time_rate: float) -> float:
        """
        Calculate risk score inversely correlated with on-time delivery rate.
        
        Business assumption: High on-time delivery → lower risk
        Formula: Inverse relationship with some noise
        
        Args:
            on_time_rate: On-time delivery percentage [60, 100]
            
        Returns:
            Risk score [0, 100] with bimodal distribution (safe 20-40, risky 60-85)
        """
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
        """
        Generate single realistic supplier record.
        
        Returns:
            Dictionary containing supplier data matching canonical schema
        """
        supplier_id = ulid.new().str
        
        # Generate correlated performance metrics
        # On-time rate: normal(μ=93, σ=5), cap [60, 100] per doc
        on_time_rate = np.clip(np.random.normal(93, 5), 60, 100)
        risk_score = self._calculate_risk_score(on_time_rate)
        financial_tier = self._determine_financial_tier(risk_score)
        
        # Geographic distribution (Zipfian skew)
        country = np.random.choice(
            list(self.country_weights.keys()),
            p=np.array(list(self.country_weights.values())) / sum(self.country_weights.values())
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
            "on_time_delivery_rate": round(on_time_rate, 2),
            "defect_rate_ppm": int(abs(np.random.normal(250, 180))),
            "capacity_units_per_week": int(abs(np.random.normal(5000, 3000))) + 100,
            "risk_score": risk_score,
            "financial_risk_tier": financial_tier,
            "certifications": certifications,
            "compliance_flags": self._generate_compliance_flags(),
            "approved_status": approved_status,
            "contracts": [f"CONTRACT_{ulid.new().str[:8]}" for _ in range(random.randint(0, 2))],
            "terms_version": f"{random.randint(1,5)}.{random.randint(0,9)}",
            "geo_coords": {
                "lat": round(self.fake.latitude(), 6),
                "lon": round(self.fake.longitude(), 6)
            },
            "data_source": "synthetic.v1",
            "source_timestamp": self._generate_source_timestamp(),
            "ingestion_timestamp": "2024-01-01T00:00:00",  # Fixed timestamp for deterministic generation
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
        """
        Generate single realistic part record with FK references to suppliers.
        
        Args:
            supplier_ids: List of valid supplier IDs for FK references
            
        Returns:
            Dictionary containing part data matching canonical schema
        """
        part_id = ulid.new().str
        
        # Category distribution from POC: ELECTRICAL 35%, MECHANICAL 35%, RAW_MATERIAL 20%, OTHER 10%
        category = np.random.choice(
            self.part_categories,
            p=self.category_weights
        )
        
        # Category-dependent lead times (ELECTRICAL longer, RAW_MATERIAL shorter)
        lead_time_params = self.category_lead_times[category]
        lead_time_avg = int(np.clip(np.random.normal(*lead_time_params), 2, 200))
        # P95 typically 15-25% higher than average
        lead_time_p95 = int(np.clip(
            np.random.normal(lead_time_avg * 1.2, lead_time_params[1]), 
            lead_time_avg + 2, 250
        ))
        
        # Qualified suppliers (1-3 suppliers per part for realistic FK testing)
        num_qualified = np.random.choice([1, 2, 3], p=[0.6, 0.3, 0.1])
        qualified_supplier_ids = list(np.random.choice(
            supplier_ids, 
            size=min(num_qualified, len(supplier_ids)), 
            replace=False
        ))
        default_supplier_id = qualified_supplier_ids[0]  # First one as default
        
        # Unit cost: log-normal distribution (wide tail for specialty parts)
        # Base cost varies by category
        category_cost_multipliers = {
            "ELECTRICAL": 1.5,      # More expensive
            "MECHANICAL": 1.2,
            "RAW_MATERIAL": 0.7,    # Cheaper commodities
            "OTHER": 1.0
        }
        base_cost = np.exp(np.random.normal(3.0, 0.8))  # Log-normal base
        unit_cost = round(base_cost * category_cost_multipliers[category], 2)
        
        # MOQ distribution (realistic ordering quantities)
        moq = np.random.choice([1, 5, 10, 25, 50, 100, 500, 1000], 
                            p=[0.15, 0.15, 0.2, 0.2, 0.15, 0.1, 0.04, 0.01])
        
        # Lifecycle status: ACTIVE 75%, NEW 10%, NRND 10%, EOL 5%
        lifecycle_status = np.random.choice(
            ["ACTIVE", "NEW", "NRND", "EOL"],
            p=[0.75, 0.10, 0.10, 0.05]
        )
        
        # Quality grade distribution (A-grade dominates)
        quality_grade = np.random.choice(["A", "B", "C"], p=[0.6, 0.3, 0.1])
        
        # Compliance flags (more for ELECTRICAL/MECHANICAL)
        compliance_prob = 0.7 if category in ["ELECTRICAL", "MECHANICAL"] else 0.3
        compliance_flags = []
        if np.random.random() < compliance_prob:
            num_flags = np.random.choice([1, 2], p=[0.8, 0.2])
            compliance_flags = list(np.random.choice(
                ["ROHS", "REACH", "ITAR", "CE"], 
                size=num_flags, 
                replace=False
            ))
        
        # UOM by category
        uom_by_category = {
            "ELECTRICAL": np.random.choice(["EA", "SET", "REEL"], p=[0.7, 0.2, 0.1]),
            "MECHANICAL": np.random.choice(["EA", "KG", "M"], p=[0.6, 0.3, 0.1]),
            "RAW_MATERIAL": np.random.choice(["KG", "M", "L", "TON"], p=[0.4, 0.3, 0.2, 0.1]),
            "OTHER": "EA"
        }
        uom = uom_by_category[category]
        
        return {
            "part_id": part_id,
            "tenant_id": self.tenant_id,
            "part_number": f"P-{random.randint(100000, 999999)}-{category[:2]}",
            "description": f"{self.fake.catch_phrase()} {category.lower()} component",
            "category": category,
            "lifecycle_status": lifecycle_status,
            "uom": uom,
            "spec_hash": f"SHA256:{ulid.new().str[:16]}",  # Simulated spec hash
            "bom_compatibility": [f"BOM_{ulid.new().str[:8]}" for _ in range(random.randint(0, 2))],
            "default_supplier_id": default_supplier_id,
            "qualified_supplier_ids": qualified_supplier_ids,
            "unit_cost": unit_cost,
            "moq": moq,
            "lead_time_days_avg": lead_time_avg,
            "lead_time_days_p95": lead_time_p95,
            "quality_grade": quality_grade,
            "compliance_flags": compliance_flags,
            "hazard_class": np.random.choice([None, "FLAMMABLE", "TOXIC"], p=[0.85, 0.10, 0.05]),
            "last_price_change": self._generate_recent_date(),
            "data_source": "synthetic.v1",
            "source_timestamp": self._generate_source_timestamp(),
            "ingestion_timestamp": "2024-01-01T00:00:00",  # Fixed for deterministic testing
            "schema_version": "1.0.0"
        }

    def _generate_recent_date(self) -> Optional[str]:
        """Generate recent price change date (or None)"""
        if np.random.random() < 0.3:  # 30% have recent price changes
            days_ago = random.randint(1, 90)
            date = (datetime.now() - timedelta(days=days_ago)).date()
            return date.isoformat()
        return None

    def generate_parts(self, supplier_ids: List[str], count: int = 30000) -> List[Dict[str, Any]]:
        """
        Generate specified number of part records with FK references to suppliers.
        
        Args:
            supplier_ids: List of valid supplier IDs for referential integrity
            count: Number of parts to generate
            
        Returns:
            List of part dictionaries matching canonical schema
        """
        if not supplier_ids:
            raise ValueError("supplier_ids cannot be empty - need suppliers for FK references")
        
        print(f"Generating {count} part records with FK references to {len(supplier_ids)} suppliers...")
        
        parts = []
        for i in range(count):
            if i % 5000 == 0:
                print(f"  Generated {i}/{count} parts...")
            
            part = self._generate_part_record(supplier_ids)
            parts.append(part)
        
        print(f"Generated {len(parts)} parts with referential integrity")
        return parts

    def inject_parts_dirty_data(self, parts: List[Dict[str, Any]], 
                            anomaly_rate: float = 0.06) -> List[Dict[str, Any]]:
        """
        Inject controlled anomalies into parts data for testing DQ gates.
        
        Part-specific anomaly types:
        - Invalid qualified_supplier_ids (orphan FKs)
        - Negative unit costs or MOQ
        - Duplicate part_number within tenant
        - Invalid UOM codes
        - Future last_price_change dates
        
        Args:
            parts: List of clean part records
            anomaly_rate: Fraction of records to corrupt (5-8% per POC)
            
        Returns:
            List of parts with injected anomalies
        """
        dirty_parts = parts.copy()
        num_anomalies = int(len(parts) * anomaly_rate)
        
        print(f"Injecting {num_anomalies} part anomalies ({anomaly_rate:.1%} of records)...")
        
        # Select random records for corruption
        dirty_indices = random.sample(range(len(parts)), num_anomalies)
        
        for idx in dirty_indices:
            part = dirty_parts[idx]
            anomaly_type = random.choice([
                "orphan_supplier_id",
                "negative_cost",
                "duplicate_part_number",
                "invalid_uom",
                "future_price_change",
                "negative_moq"
            ])
            
            if anomaly_type == "orphan_supplier_id":
                # Create non-existent supplier ID
                part["default_supplier_id"] = ulid.new().str
                part["qualified_supplier_ids"] = [ulid.new().str]
            elif anomaly_type == "negative_cost":
                part["unit_cost"] = -random.uniform(1, 100)
            elif anomaly_type == "duplicate_part_number":
                # Find another part's number to duplicate
                if len(parts) > 1:
                    other_part = random.choice([p for p in parts if p != part])
                    part["part_number"] = other_part["part_number"]
            elif anomaly_type == "invalid_uom":
                part["uom"] = "INVALID_UOM"
            elif anomaly_type == "future_price_change":
                future_date = datetime.now() + timedelta(days=random.randint(1, 30))
                part["last_price_change"] = future_date.date().isoformat()
            elif anomaly_type == "negative_moq":
                part["moq"] = -random.randint(1, 100)
        
        print(f"Injected part anomalies in {len(dirty_indices)} records")
        return dirty_parts


    def generate_suppliers(self, count: int = 30000) -> List[Dict[str, Any]]:
        """
        Generate specified number of supplier records.
        
        Args:
            count: Number of suppliers to generate
            
        Returns:
            List of supplier dictionaries matching canonical schema
        """
        print(f"Generating {count} supplier records with seed={self.seed}...")
        
        suppliers = []
        for i in range(count):
            if i % 5000 == 0:
                print(f"  Generated {i}/{count} suppliers...")
            
            supplier = self._generate_supplier_record()
            suppliers.append(supplier)
        
        print(f"Generated {len(suppliers)} suppliers")
        return suppliers
    
    def inject_dirty_data(self, suppliers: List[Dict[str, Any]], 
                         anomaly_rate: float = 0.06) -> List[Dict[str, Any]]:
        """
        Inject controlled anomalies for testing data quality gates.
        
        Anomaly types per doc section 3.3:
        - Out-of-range rates (e.g., 104%)
        - Missing emails/phones
        - Bogus country codes
        - Duplicate supplier_code within tenant
        - Future source_timestamp
        
        Args:
            suppliers: List of clean supplier records
            anomaly_rate: Fraction of records to corrupt (5-8% per doc)
            
        Returns:
            List of suppliers with injected anomalies
        """
        dirty_suppliers = suppliers.copy()
        num_anomalies = int(len(suppliers) * anomaly_rate)
        
        print(f"Injecting {num_anomalies} anomalies ({anomaly_rate:.1%} of records)...")
        
        # Select random records for corruption
        dirty_indices = random.sample(range(len(suppliers)), num_anomalies)
        
        for idx in dirty_indices:
            supplier = dirty_suppliers[idx]
            anomaly_type = random.choice([
                "out_of_range_rate",
                "missing_contact", 
                "bogus_country",
                "duplicate_code",
                "future_timestamp"
            ])
            
            if anomaly_type == "out_of_range_rate":
                supplier["on_time_delivery_rate"] = 104.0  # Invalid rate
            elif anomaly_type == "missing_contact":
                supplier["contact_email"] = None
                supplier["contact_phone"] = None
            elif anomaly_type == "bogus_country":
                supplier["country"] = "XX"  # Invalid ISO code
            elif anomaly_type == "duplicate_code":
                # Find another supplier's code to duplicate
                if len(suppliers) > 1:
                    other_supplier = random.choice([s for s in suppliers if s != supplier])
                    supplier["supplier_code"] = other_supplier["supplier_code"]
            elif anomaly_type == "future_timestamp":
                future_date = datetime.now() + timedelta(days=random.randint(1, 30))
                supplier["source_timestamp"] = future_date.isoformat()
        
        print(f"Injected anomalies in {len(dirty_indices)} records")
        return dirty_suppliers
    
    def export_to_parquet(self, data: List[Dict[str, Any]], filename: str):
        """
        Export generated data to Parquet file with proper schema and compression.
        
        Args:
            data: List of records to export
            filename: Output Parquet filename
        """
        if not data:
            print("No data to export")
            return
        
        print(f"Exporting {len(data)} records to {filename}...")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Handle nested structures properly for Parquet
        if 'geo_coords' in df.columns:
            df['geo_lat'] = df['geo_coords'].apply(lambda x: x['lat'] if x else None)
            df['geo_lon'] = df['geo_coords'].apply(lambda x: x['lon'] if x else None)
            df = df.drop('geo_coords', axis=1)
        
        # Convert arrays to strings (Parquet doesn't handle mixed arrays well)
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if column contains lists
                sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample_val, list):
                    df[col] = df[col].apply(lambda x: ','.join(map(str, x)) if x else '')
        
        # Export with compression and proper row group size (128MB as per doc)
        df.to_parquet(
            filename,
            compression='snappy',
            row_group_size=50000,  # Adjust based on record size
            index=False
        )
        
        print(f"Exported to {filename}")
    
    def export_to_csv(self, data: List[Dict[str, Any]], filename: str):
        """
        Export generated data to CSV file (for debugging/inspection only).
        
        Args:
            data: List of records to export
            filename: Output CSV filename
        """
        if not data:
            print("No data to export")
            return
        
        print(f"Exporting {len(data)} records to {filename}...")
        
        # Flatten nested structures (geo_coords, certifications, etc.)
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
                    flat_record[key] = ','.join(map(str, value)) if value else ''
            
            flattened_data.append(flat_record)
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if flattened_data:
                fieldnames = flattened_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_data)
        
        print(f"Exported to {filename}")


# Example usage
if __name__ == "__main__":
    generator = Generator(seed=42, tenant_id="tenant_acme")
    # Generate clean suppliers
    suppliers = generator.generate_suppliers(count=100000)  # Start small for testing
    # Inject dirty data
    dirty_suppliers = generator.inject_dirty_data(suppliers, anomaly_rate=0.06)
    # Export to Parquet (primary format for pipeline)
    generator.export_to_parquet(dirty_suppliers, "suppliers_test.parquet")# Updated main execution block for generator.py
if __name__ == "__main__":
    generator = Generator(seed=42, tenant_id="tenant_acme")
    
    print("=== Supply Chain Data Generation POC ===")
    
    # Generate suppliers first (needed for FK references)
    print("\n1. Generating suppliers...")
    suppliers = generator.generate_suppliers(count=30000)
    
    # Extract supplier IDs for parts FK references
    supplier_ids = [s["supplier_id"] for s in suppliers]
    print(f"   Created {len(supplier_ids)} supplier IDs for FK references")
    
    # Generate parts with FK references
    print("\n2. Generating parts...")
    parts = generator.generate_parts(supplier_ids, count=30000)
    
    # Inject dirty data into both datasets
    print("\n3. Injecting dirty data...")
    dirty_suppliers = generator.inject_dirty_data(suppliers, anomaly_rate=0.06)
    dirty_parts = generator.inject_parts_dirty_data(parts, anomaly_rate=0.06)
    
    # Export to Parquet (primary format for pipeline)
    print("\n4. Exporting data...")
    generator.export_to_parquet(dirty_suppliers, "suppliers_test.parquet")
    generator.export_to_parquet(dirty_parts, "parts_test.parquet")
    
    # Optional: Export small CSV samples for inspection
    generator.export_to_csv(dirty_suppliers[:100], "suppliers_sample.csv")
    generator.export_to_csv(dirty_parts[:100], "parts_sample.csv")
    
    print(f"\n=== Generation Complete ===")
    print(f"Suppliers: {len(dirty_suppliers)} records")
    print(f"Parts: {len(dirty_parts)} records") 
    print(f"FK integrity maintained for {len([p for p in parts if p['default_supplier_id'] in supplier_ids])} parts")
    
    # Validate some basic assumptions
    print("\n=== Validation Summary ===")
    
    # Supplier validation
    risk_low = len([s for s in suppliers if s['risk_score'] < 35])
    risk_high = len([s for s in suppliers if s['risk_score'] >= 60])
    print(f"Suppliers - Low risk (<35): {risk_low} ({risk_low/len(suppliers):.1%})")
    print(f"Suppliers - High risk (>=60): {risk_high} ({risk_high/len(suppliers):.1%})")
    
    # Parts validation  
    electrical_parts = len([p for p in parts if p['category'] == 'ELECTRICAL'])
    active_parts = len([p for p in parts if p['lifecycle_status'] == 'ACTIVE'])
    print(f"Parts - ELECTRICAL category: {electrical_parts} ({electrical_parts/len(parts):.1%})")
    print(f"Parts - ACTIVE lifecycle: {active_parts} ({active_parts/len(parts):.1%})")
    
    # FK integrity check
    valid_fks = len([p for p in parts 
                    if p['default_supplier_id'] in supplier_ids and
                    all(sid in supplier_ids for sid in p['qualified_supplier_ids'])])
    print(f"Parts with valid FK references: {valid_fks} ({valid_fks/len(parts):.1%})")