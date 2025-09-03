"""
Test suite for Supply Chain Data Generator

Tests cover schema validation, business logic correlations, dirty data injection,
export functionality, and performance characteristics.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import time

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generator.generator import Generator


@pytest.fixture
def generator():
    """Create generator with fixed seed for testing"""
    return Generator(seed=123, tenant_id="test_tenant")


# Basic Generation Tests
def test_generate_suppliers_count(generator):
    """Test basic supplier count generation"""
    suppliers = generator.generate_suppliers(count=10)
    assert len(suppliers) == 10


def test_generate_suppliers_empty(generator):
    """Test edge case of zero suppliers"""
    suppliers = generator.generate_suppliers(count=0)
    assert len(suppliers) == 0


# Schema Validation Tests
def test_supplier_complete_schema(generator):
    """Test ALL required fields from canonical schema are present"""
    supplier = generator.generate_suppliers(count=1)[0]
    
    required_keys = [
        "supplier_id", "supplier_code", "tenant_id", "legal_name", "dba_name",
        "country", "region", "address_line1", "address_line2", "city", "state",
        "postal_code", "contact_email", "contact_phone", "preferred_currency",
        "incoterms", "lead_time_days_avg", "lead_time_days_p95", 
        "on_time_delivery_rate", "defect_rate_ppm", "capacity_units_per_week",
        "risk_score", "financial_risk_tier", "certifications", "compliance_flags",
        "approved_status", "contracts", "terms_version", "geo_coords",
        "data_source", "source_timestamp", "ingestion_timestamp", "schema_version"
    ]
    
    for key in required_keys:
        assert key in supplier, f"Missing required field: {key}"


def test_supplier_data_types(generator):
    """Test data types match expected schema"""
    supplier = generator.generate_suppliers(count=1)[0]
    
    # String fields
    assert isinstance(supplier["supplier_id"], str)
    assert isinstance(supplier["legal_name"], str)
    assert isinstance(supplier["country"], str)
    
    # Numeric fields
    assert isinstance(supplier["lead_time_days_avg"], int)
    assert isinstance(supplier["on_time_delivery_rate"], float)
    assert isinstance(supplier["risk_score"], float)
    
    # Array fields
    assert isinstance(supplier["certifications"], list)
    assert isinstance(supplier["compliance_flags"], list)
    assert isinstance(supplier["contracts"], list)
    
    # Nested structure
    assert isinstance(supplier["geo_coords"], dict)
    assert "lat" in supplier["geo_coords"]
    assert "lon" in supplier["geo_coords"]


# Business Logic Tests
def test_deterministic_generation():
    """Test same seed produces identical output"""
    gen1 = Generator(seed=42)
    gen2 = Generator(seed=42)
    
    s1 = gen1.generate_suppliers(count=5)
    s2 = gen2.generate_suppliers(count=5)
    
    assert s1 == s2, "Same seed should produce identical suppliers"


def test_risk_score_correlation(generator):
    """Test actual correlation between on-time rate and risk score"""
    suppliers = generator.generate_suppliers(count=200)
    
    rates = [s["on_time_delivery_rate"] for s in suppliers]
    risks = [s["risk_score"] for s in suppliers]
    
    # Calculate Pearson correlation coefficient
    correlation = np.corrcoef(rates, risks)[0, 1]
    
    # Should be negative correlation (high on-time = low risk)
    assert correlation < -0.3, f"Risk-OnTime correlation too weak: {correlation:.3f}"
    
    # Verify ranges
    assert all(60 <= rate <= 100 for rate in rates), "On-time rates outside [60,100] range"
    assert all(0 <= risk <= 100 for risk in risks), "Risk scores outside [0,100] range"


def test_country_distribution_zipfian(generator):
    """Test Zipfian country distribution works correctly"""
    suppliers = generator.generate_suppliers(count=1000)
    countries = [s["country"] for s in suppliers]
    
    # Count occurrences
    country_counts = {}
    for country in countries:
        country_counts[country] = country_counts.get(country, 0) + 1
    
    # Sort by frequency
    sorted_countries = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)
    
    # CN should be most common (~22%), US second (~18%)
    assert sorted_countries[0][0] == "CN", f"Expected CN most common, got {sorted_countries[0][0]}"
    assert sorted_countries[1][0] == "US", f"Expected US second, got {sorted_countries[1][0]}"
    
    # CN should have roughly 22% (220 ± 50 for 1000 records)
    cn_count = country_counts.get("CN", 0)
    assert 170 <= cn_count <= 270, f"CN count {cn_count} outside expected range [170,270]"


def test_financial_risk_tier_mapping(generator):
    """Test risk score maps correctly to financial tiers"""
    suppliers = generator.generate_suppliers(count=100)
    
    for supplier in suppliers:
        risk_score = supplier["risk_score"]
        tier = supplier["financial_risk_tier"]
        
        if 0 <= risk_score < 35:
            assert tier == "LOW", f"Risk {risk_score} should map to LOW, got {tier}"
        elif 35 <= risk_score < 60:
            assert tier == "MEDIUM", f"Risk {risk_score} should map to MEDIUM, got {tier}"
        else:
            assert tier == "HIGH", f"Risk {risk_score} should map to HIGH, got {tier}"


# Dirty Data Tests
def test_dirty_data_injection_rate(generator):
    """Test dirty data injection rate is within expected bounds"""
    suppliers = generator.generate_suppliers(count=100)
    dirty = generator.inject_dirty_data(suppliers, anomaly_rate=0.06)
    
    now = datetime.now()
    anomaly_count = 0
    
    for supplier in dirty:
        is_anomaly = (
            supplier["on_time_delivery_rate"] == 104.0 or
            supplier["contact_email"] is None or
            supplier["country"] == "XX" or
            (supplier["source_timestamp"] and 
             datetime.fromisoformat(supplier["source_timestamp"].replace('Z', '+00:00').split('+')[0]) > now)
        )
        if is_anomaly:
            anomaly_count += 1
    
    # 6% ± tolerance (4-8% for 100 records)
    assert 4 <= anomaly_count <= 8, f"Anomaly count {anomaly_count} outside expected range [4,8]"


def test_dirty_data_types(generator):
    """Test specific dirty data anomaly types"""
    suppliers = generator.generate_suppliers(count=200)
    dirty = generator.inject_dirty_data(suppliers, anomaly_rate=0.10)  # Higher rate for testing
    
    # Count each anomaly type
    out_of_range = sum(1 for s in dirty if s["on_time_delivery_rate"] == 104.0)
    missing_contact = sum(1 for s in dirty if s["contact_email"] is None)
    bogus_country = sum(1 for s in dirty if s["country"] == "XX")
    
    # Should have at least some of each type with 10% anomaly rate (allow some variance)
    total_anomalies = out_of_range + missing_contact + bogus_country
    assert total_anomalies >= 12, f"Total anomalies {total_anomalies} too low for 10% rate"


def test_supplier_code_uniqueness_clean(generator):
    """Test supplier codes are unique in clean data"""
    suppliers = generator.generate_suppliers(count=100)
    codes = [s["supplier_code"] for s in suppliers]
    
    assert len(set(codes)) == len(codes), "Supplier codes should be unique"


# Export Tests
def test_export_to_parquet(tmp_path, generator):
    """Test primary export format - Parquet"""
    suppliers = generator.generate_suppliers(count=10)
    file_path = tmp_path / "test_suppliers.parquet"
    
    generator.export_to_parquet(suppliers, str(file_path))
    
    # Verify file exists and is readable
    assert file_path.exists(), "Parquet file was not created"
    
    # Load and verify data
    df = pd.read_parquet(file_path)
    assert len(df) == 10, f"Expected 10 records, got {len(df)}"
    
    # Check key columns exist
    expected_cols = ["supplier_id", "tenant_id", "country", "risk_score"]
    for col in expected_cols:
        assert col in df.columns, f"Missing column {col} in Parquet export"


def test_export_to_csv(tmp_path, generator):
    """Test CSV export for debugging"""
    suppliers = generator.generate_suppliers(count=5)
    file_path = tmp_path / "test_suppliers.csv"
    
    generator.export_to_csv(suppliers, str(file_path))
    
    # Verify file exists
    assert file_path.exists(), "CSV file was not created"
    
    # Check content
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    
    assert len(lines) >= 6, f"Expected header + 5 data rows, got {len(lines)} lines"  # Header + data
    assert "supplier_id" in lines[0], "CSV header should contain supplier_id"


def test_export_empty_data(tmp_path, generator):
    """Test export with empty dataset"""
    file_path = tmp_path / "empty.parquet"
    generator.export_to_parquet([], str(file_path))
    
    # Should handle gracefully without creating file
    assert not file_path.exists(), "Should not create file for empty dataset"


# Performance Tests
def test_generation_speed_small(generator):
    """Test generation performance for small dataset"""
    start = time.time()
    suppliers = generator.generate_suppliers(count=1000)
    elapsed = time.time() - start
    
    assert elapsed < 10, f"Generation too slow: {elapsed:.2f}s for 1k records"
    assert len(suppliers) == 1000


def test_generation_speed_medium():
    """Test generation performance for medium dataset"""
    gen = Generator(seed=42)
    
    start = time.time()
    suppliers = gen.generate_suppliers(count=10000)
    elapsed = time.time() - start
    
    assert elapsed < 60, f"Generation too slow: {elapsed:.2f}s for 10k records"
    assert len(suppliers) == 10000


# Integration Tests
def test_full_pipeline_integration(tmp_path, generator):
    """Test complete generation -> dirty injection -> export pipeline"""
    # Generate clean data
    suppliers = generator.generate_suppliers(count=100)
    assert len(suppliers) == 100
    
    # Inject dirty data
    dirty_suppliers = generator.inject_dirty_data(suppliers, anomaly_rate=0.06)
    assert len(dirty_suppliers) == 100
    
    # Export to both formats
    parquet_path = tmp_path / "integration_test.parquet"
    csv_path = tmp_path / "integration_test.csv"
    
    generator.export_to_parquet(dirty_suppliers, str(parquet_path))
    generator.export_to_csv(dirty_suppliers, str(csv_path))
    
    # Verify both files created successfully
    assert parquet_path.exists()
    assert csv_path.exists()
    
    # Load and verify Parquet data
    df = pd.read_parquet(parquet_path)
    assert len(df) == 100
    
    # Verify data integrity
    assert df["tenant_id"].nunique() == 1  # Single tenant
    assert df["supplier_id"].nunique() == 100  # Unique IDs


# Edge Case Tests
def test_certification_field_validation(generator):
    """Test certifications field contains valid values"""
    suppliers = generator.generate_suppliers(count=50)
    valid_certs = {"ISO9001", "IATF16949", "AS9100", "ISO14001"}
    
    for supplier in suppliers:
        certs = supplier["certifications"]
        assert isinstance(certs, list), "Certifications should be a list"
        
        for cert in certs:
            assert cert in valid_certs, f"Invalid certification: {cert}"


def test_ulid_format_validation(generator):
    """Test ULID format for supplier IDs"""
    suppliers = generator.generate_suppliers(count=10)
    
    for supplier in suppliers:
        supplier_id = supplier["supplier_id"]
        
        # ULID should be 26 characters
        assert len(supplier_id) == 26, f"ULID length should be 26, got {len(supplier_id)}"
        
        # Should only contain valid ULID characters (Crockford's Base32)
        valid_chars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        assert all(c in valid_chars for c in supplier_id), f"Invalid ULID characters in {supplier_id}"


def test_timestamp_format_validation(generator):
    """Test timestamp formats are ISO-8601 compliant"""
    suppliers = generator.generate_suppliers(count=5)
    
    for supplier in suppliers:
        source_ts = supplier["source_timestamp"]
        ingestion_ts = supplier["ingestion_timestamp"]
        
        # Should be parseable as ISO format
        try:
            datetime.fromisoformat(source_ts.replace('Z', '+00:00'))
            datetime.fromisoformat(ingestion_ts.replace('Z', '+00:00'))
        except ValueError as e:
            pytest.fail(f"Invalid timestamp format: {e}")


# Large Scale Test (commented out for regular testing)
# Uncomment and run manually when needed

def test_large_scale_generation():
    '''Test full-scale generation (30k records) - run manually'''
    gen = Generator(seed=42)
    
    start = time.time()
    suppliers = gen.generate_suppliers(count=30000)
    generation_time = time.time() - start
    
    # Should complete in reasonable time (adjust threshold as needed)
    assert generation_time < 300, f"30k generation took too long: {generation_time:.1f}s"
    assert len(suppliers) == 30000
    
    # Test data quality at scale
    countries = [s["country"] for s in suppliers]
    cn_pct = countries.count("CN") / len(countries)
    assert 0.15 <= cn_pct <= 0.30, f"CN percentage {cn_pct:.3f} outside expected range"