"""
Complete Test Suite for Supply Chain Data Generator

Tests cover schema validation, business logic correlations, dirty data injection,
export functionality, performance characteristics, parts generation, and referential integrity.
"""

from data_generator.generator import Generator
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import time

# Add project root to Python path - handle different OS path separators
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


@pytest.fixture
def generator():
    """Create generator with fixed seed for testing"""
    return Generator(seed=123, tenant_id="test_tenant")


# Basic Generation Tests
def test_generate_suppliers_count(generator):
    """Test basic supplier count generation"""
    print("\n=== Testing supplier count generation ===")
    start = time.time()
    suppliers = generator.generate_suppliers(count=10)
    elapsed = time.time() - start
    print(f"Generated {len(suppliers)} suppliers in {elapsed:.3f}s")
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
# Note: Deterministic generation test skipped due to ULID timestamp component
# ULID uses current timestamp making identical generation impossible with same seed
"""
def test_deterministic_generation():
    '''Test same seed produces identical output - SKIPPED due to ULID timestamps'''
    gen1 = Generator(seed=42)
    gen2 = Generator(seed=42)
    
    s1 = gen1.generate_suppliers(count=5)
    s2 = gen2.generate_suppliers(count=5)
    
    assert s1 == s2, "Same seed should produce identical suppliers"
"""


def test_risk_score_correlation(generator):
    """Test actual correlation between on-time rate and risk score"""
    print("\n=== Testing risk score correlation ===")
    start = time.time()
    suppliers = generator.generate_suppliers(count=200)
    elapsed = time.time() - start
    print(f"Generated 200 suppliers in {elapsed:.3f}s")

    rates = [s["on_time_delivery_rate"] for s in suppliers]
    risks = [s["risk_score"] for s in suppliers]

    # Calculate Pearson correlation coefficient
    correlation = np.corrcoef(rates, risks)[0, 1]
    print(f"On-time vs Risk correlation: {correlation:.3f}")

    # Should be negative correlation (high on-time = low risk)
    assert correlation < - \
        0.3, f"Risk-OnTime correlation too weak: {correlation:.3f}"

    # Verify ranges
    assert all(
        60 <= rate <= 100 for rate in rates), "On-time rates outside [60,100] range"
    assert all(
        0 <= risk <= 100 for risk in risks), "Risk scores outside [0,100] range"


def test_country_distribution_zipfian(generator):
    """Test Zipfian country distribution works correctly"""
    print("\n=== Testing country distribution ===")
    start = time.time()
    suppliers = generator.generate_suppliers(count=1000)
    elapsed = time.time() - start
    print(f"Generated 1000 suppliers in {elapsed:.3f}s")

    countries = [s["country"] for s in suppliers]

    # Count occurrences
    country_counts = {}
    for country in countries:
        country_counts[country] = country_counts.get(country, 0) + 1

    # Sort by frequency
    sorted_countries = sorted(country_counts.items(),
                              key=lambda x: x[1], reverse=True)

    print(f"Top 3 countries: {sorted_countries[:3]}")

    # CN should be most common (~22%), US second (~18%)
    assert sorted_countries[0][
        0] == "CN", f"Expected CN most common, got {sorted_countries[0][0]}"
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


# Parts Generation Tests
def test_generate_parts_count(generator):
    """Test basic parts count generation"""
    print("\n=== Testing parts generation ===")
    start = time.time()
    suppliers = generator.generate_suppliers(count=10)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    parts = generator.generate_parts(count=20, supplier_ids=supplier_ids)
    elapsed = time.time() - start
    print(f"Generated 10 suppliers + 20 parts in {elapsed:.3f}s")
    assert len(parts) == 20


def test_generate_parts_requires_suppliers(generator):
    """Test parts generation requires supplier IDs"""
    with pytest.raises(ValueError, match="supplier_ids required"):
        generator.generate_parts(count=10, supplier_ids=None)

    with pytest.raises(ValueError, match="At least one supplier_id required"):
        generator.generate_parts(count=10, supplier_ids=[])


def test_part_complete_schema(generator):
    """Test ALL required fields from parts canonical schema"""
    suppliers = generator.generate_suppliers(count=5)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    part = generator.generate_parts(count=1, supplier_ids=supplier_ids)[0]

    required_keys = [
        "part_id", "tenant_id", "part_number", "description", "category",
        "lifecycle_status", "uom", "spec_hash", "bom_compatibility",
        "default_supplier_id", "qualified_supplier_ids", "unit_cost", "moq",
        "lead_time_days_avg", "lead_time_days_p95", "quality_grade",
        "compliance_flags", "hazard_class", "last_price_change",
        "data_source", "source_timestamp", "ingestion_timestamp", "schema_version"
    ]

    for key in required_keys:
        assert key in part, f"Missing required field: {key}"


def test_part_data_types(generator):
    """Test part data types match expected schema"""
    suppliers = generator.generate_suppliers(count=5)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    part = generator.generate_parts(count=1, supplier_ids=supplier_ids)[0]

    # String fields
    assert isinstance(part["part_id"], str)
    assert isinstance(part["part_number"], str)
    assert isinstance(part["category"], str)
    assert isinstance(part["default_supplier_id"], str)

    # Numeric fields (accept both Python and NumPy types for now)
    assert isinstance(part["unit_cost"], (float, np.floating))
    assert isinstance(part["moq"], (int, np.integer))
    assert isinstance(part["lead_time_days_avg"], (int, np.integer))
    assert isinstance(part["lead_time_days_p95"], (int, np.integer))

    # Array fields
    assert isinstance(part["qualified_supplier_ids"], list)
    assert isinstance(part["compliance_flags"], list)
    assert isinstance(part["bom_compatibility"], list)


def test_part_category_distribution(generator):
    """Test part category distribution matches spec"""
    print("\n=== Testing part category distribution ===")
    suppliers = generator.generate_suppliers(count=10)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    start = time.time()
    parts = generator.generate_parts(count=1000, supplier_ids=supplier_ids)
    elapsed = time.time() - start
    print(f"Generated 1000 parts in {elapsed:.3f}s")

    categories = [p["category"] for p in parts]

    # Count occurrences
    cat_counts = {}
    for cat in categories:
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    total = len(categories)
    print(f"Category distribution: {cat_counts}")

    # Check distributions (ELECTRICAL 35%, MECHANICAL 35%, RAW_MATERIAL 20%, OTHER 10%)
    electrical_pct = cat_counts.get("ELECTRICAL", 0) / total
    mechanical_pct = cat_counts.get("MECHANICAL", 0) / total
    raw_material_pct = cat_counts.get("RAW_MATERIAL", 0) / total
    other_pct = cat_counts.get("OTHER", 0) / total

    assert 0.30 <= electrical_pct <= 0.40, f"ELECTRICAL {electrical_pct:.3f} outside [0.30, 0.40]"
    assert 0.30 <= mechanical_pct <= 0.40, f"MECHANICAL {mechanical_pct:.3f} outside [0.30, 0.40]"
    assert 0.15 <= raw_material_pct <= 0.25, f"RAW_MATERIAL {raw_material_pct:.3f} outside [0.15, 0.25]"
    assert 0.05 <= other_pct <= 0.15, f"OTHER {other_pct:.3f} outside [0.05, 0.15]"


def test_part_lead_time_correlation(generator):
    """Test parts lead time correlation by category"""
    print("\n=== Testing part lead time correlation ===")
    suppliers = generator.generate_suppliers(count=10)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    parts = generator.generate_parts(count=500, supplier_ids=supplier_ids)

    # Group by category
    electrical_times = [p["lead_time_days_avg"]
                        for p in parts if p["category"] == "ELECTRICAL"]
    raw_material_times = [p["lead_time_days_avg"]
                          for p in parts if p["category"] == "RAW_MATERIAL"]

    if electrical_times and raw_material_times:
        avg_electrical = sum(electrical_times) / len(electrical_times)
        avg_raw_material = sum(raw_material_times) / len(raw_material_times)

        print(
            f"Average lead times - ELECTRICAL: {avg_electrical:.1f}, RAW_MATERIAL: {avg_raw_material:.1f}")

        # ELECTRICAL should have longer lead times than RAW_MATERIAL
        assert avg_electrical > avg_raw_material, f"ELECTRICAL ({avg_electrical:.1f}) should > RAW_MATERIAL ({avg_raw_material:.1f})"


def test_part_lifecycle_distribution(generator):
    """Test part lifecycle status distribution"""
    suppliers = generator.generate_suppliers(count=10)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    parts = generator.generate_parts(count=400, supplier_ids=supplier_ids)
    lifecycles = [p["lifecycle_status"] for p in parts]

    # Count occurrences
    lifecycle_counts = {}
    for status in lifecycles:
        lifecycle_counts[status] = lifecycle_counts.get(status, 0) + 1

    total = len(lifecycles)

    # Check distributions (ACTIVE 75%, NEW 10%, NRND 10%, EOL 5%)
    active_pct = lifecycle_counts.get("ACTIVE", 0) / total
    new_pct = lifecycle_counts.get("NEW", 0) / total
    nrnd_pct = lifecycle_counts.get("NRND", 0) / total
    eol_pct = lifecycle_counts.get("EOL", 0) / total

    assert 0.70 <= active_pct <= 0.80, f"ACTIVE {active_pct:.3f} outside [0.70, 0.80]"
    assert 0.05 <= new_pct <= 0.15, f"NEW {new_pct:.3f} outside [0.05, 0.15]"
    assert 0.05 <= nrnd_pct <= 0.15, f"NRND {nrnd_pct:.3f} outside [0.05, 0.15]"
    assert 0.02 <= eol_pct <= 0.08, f"EOL {eol_pct:.3f} outside [0.02, 0.08]"


# Referential Integrity Tests
def test_referential_integrity_basic(generator):
    """Test parts properly reference existing suppliers"""
    print("\n=== Testing referential integrity ===")
    suppliers = generator.generate_suppliers(count=20)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    parts = generator.generate_parts(count=50, supplier_ids=supplier_ids)

    for part in parts:
        # Default supplier should be in the available list
        assert part[
            "default_supplier_id"] in supplier_ids, f"Invalid default_supplier_id: {part['default_supplier_id']}"

        # All qualified suppliers should be in the available list
        for qualified_id in part["qualified_supplier_ids"]:
            assert qualified_id in supplier_ids, f"Invalid qualified_supplier_id: {qualified_id}"

        # Default supplier should be in qualified list
        assert part["default_supplier_id"] in part["qualified_supplier_ids"], "Default supplier not in qualified list"


def test_referential_integrity_comprehensive(generator):
    """Test referential integrity across full dataset"""
    suppliers = generator.generate_suppliers(count=100)
    supplier_ids = [s["supplier_id"] for s in suppliers]
    supplier_id_set = set(supplier_ids)

    parts = generator.generate_parts(count=200, supplier_ids=supplier_ids)

    # Check all FK references
    orphan_defaults = 0
    orphan_qualified = 0

    for part in parts:
        if part["default_supplier_id"] not in supplier_id_set:
            orphan_defaults += 1

        for qual_id in part["qualified_supplier_ids"]:
            if qual_id not in supplier_id_set:
                orphan_qualified += 1

    # Should have 0 orphans in clean data
    assert orphan_defaults == 0, f"Found {orphan_defaults} orphan default_supplier_ids"
    assert orphan_qualified == 0, f"Found {orphan_qualified} orphan qualified_supplier_ids"


def test_qualified_suppliers_logic(generator):
    """Test qualified suppliers logic and distribution"""
    suppliers = generator.generate_suppliers(count=50)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    parts = generator.generate_parts(count=100, supplier_ids=supplier_ids)

    qualified_counts = [len(p["qualified_supplier_ids"]) for p in parts]

    # Should have 1-3 qualified suppliers per part
    assert all(
        1 <= count <= 3 for count in qualified_counts), "Qualified supplier count outside [1,3] range"

    # Distribution should favor 1 supplier (60% probability)
    one_supplier = sum(1 for count in qualified_counts if count == 1)
    pct_one = one_supplier / len(qualified_counts)
    assert 0.50 <= pct_one <= 0.70, f"Single supplier percentage {pct_one:.3f} outside expected range"


# Dirty Data Tests
def test_dirty_data_injection_rate(generator):
    """Test dirty data injection rate is within expected bounds"""
    suppliers = generator.generate_suppliers(count=100)
    dirty = generator.inject_dirty_data(
        suppliers, data_type="suppliers", anomaly_rate=0.06)

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
    dirty = generator.inject_dirty_data(
        suppliers, data_type="suppliers", anomaly_rate=0.10)  # Higher rate for testing

    # Count each anomaly type
    out_of_range = sum(1 for s in dirty if s["on_time_delivery_rate"] == 104.0)
    missing_contact = sum(1 for s in dirty if s["contact_email"] is None)
    bogus_country = sum(1 for s in dirty if s["country"] == "XX")

    # Should have at least some of each type with 10% anomaly rate (allow some variance)
    total_anomalies = out_of_range + missing_contact + bogus_country
    assert total_anomalies >= 12, f"Total anomalies {total_anomalies} too low for 10% rate"


def test_parts_dirty_data_injection(generator):
    """Test parts-specific dirty data injection"""
    print("\n=== Testing parts dirty data injection ===")
    suppliers = generator.generate_suppliers(count=20)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    parts = generator.generate_parts(count=100, supplier_ids=supplier_ids)
    dirty_parts = generator.inject_dirty_data(
        parts, data_type="parts", anomaly_rate=0.08)

    # Count part-specific anomalies
    invalid_supplier_ids = 0
    negative_costs = 0
    wrong_uoms = 0
    duplicate_part_numbers = 0

    supplier_id_set = set(supplier_ids)
    part_numbers = []

    for part in dirty_parts:
        # Check for invalid supplier IDs (orphan FKs)
        if part["default_supplier_id"] not in supplier_id_set:
            invalid_supplier_ids += 1

        # Check for negative costs
        if part["unit_cost"] < 0:
            negative_costs += 1

        # Check for wrong UOMs
        if part["uom"] == "INVALID_UOM":
            wrong_uoms += 1

        # Collect part numbers for duplicate check
        part_numbers.append(part["part_number"])

    # Check for duplicates
    duplicate_part_numbers = len(part_numbers) - len(set(part_numbers))

    total_anomalies = invalid_supplier_ids + \
        negative_costs + wrong_uoms + duplicate_part_numbers
    print(
        f"Part anomalies: orphan_fks={invalid_supplier_ids}, negative_costs={negative_costs}, wrong_uoms={wrong_uoms}, duplicates={duplicate_part_numbers}")

    # Should have roughly 8% anomalies (6-10% tolerance)
    assert 6 <= total_anomalies <= 10, f"Total part anomalies {total_anomalies} outside expected range [6,10]"


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

    # Header + data
    assert len(
        lines) >= 6, f"Expected header + 5 data rows, got {len(lines)} lines"
    assert "supplier_id" in lines[0], "CSV header should contain supplier_id"


def test_export_empty_data(tmp_path, generator):
    """Test export with empty dataset"""
    file_path = tmp_path / "empty.parquet"
    generator.export_to_parquet([], str(file_path))

    # Should handle gracefully without creating file
    assert not file_path.exists(), "Should not create file for empty dataset"


# Performance Tests
def test_supplier_generation_performance_benchmark():
    """Benchmark supplier generation performance"""
    print("\n=== PERFORMANCE BENCHMARKS ===")
    gen = Generator(seed=42)

    # Test different sizes
    sizes_and_limits = [(1000, 3), (5000, 10), (10000, 20)]

    for size, time_limit in sizes_and_limits:
        start = time.time()
        suppliers = gen.generate_suppliers(count=size)
        elapsed = time.time() - start

        print(f"{size} suppliers: {elapsed:.3f}s (limit: {time_limit}s)")

        assert len(
            suppliers) == size, f"Generated {len(suppliers)}, expected {size}"
        assert elapsed < time_limit, f"{size} suppliers took {elapsed:.2f}s, limit {time_limit}s"

        # Verify data quality at scale
        countries = [s["country"] for s in suppliers]
        cn_count = countries.count("CN")
        cn_pct = cn_count / len(countries)
        assert 0.15 <= cn_pct <= 0.30, f"CN percentage {cn_pct:.3f} outside range at {size} scale"


def test_parts_generation_performance_benchmark():
    """Benchmark parts generation performance"""
    print("\n=== PARTS PERFORMANCE BENCHMARKS ===")
    gen = Generator(seed=42)

    # Generate suppliers first
    suppliers = gen.generate_suppliers(count=1000)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    # Test parts generation performance
    sizes_and_limits = [(1000, 3), (5000, 12), (10000, 25)]

    for size, time_limit in sizes_and_limits:
        start = time.time()
        parts = gen.generate_parts(count=size, supplier_ids=supplier_ids)
        elapsed = time.time() - start

        print(f"{size} parts: {elapsed:.3f}s (limit: {time_limit}s)")

        assert len(parts) == size, f"Generated {len(parts)}, expected {size}"
        assert elapsed < time_limit, f"{size} parts took {elapsed:.2f}s, limit {time_limit}s"


def test_export_performance_benchmark(tmp_path):
    """Benchmark export performance for different formats"""
    print("\n=== EXPORT PERFORMANCE BENCHMARKS ===")
    gen = Generator(seed=42)

    # Generate moderate dataset
    suppliers = gen.generate_suppliers(count=2000)

    # Test Parquet export performance
    parquet_path = tmp_path / "perf_test.parquet"
    start = time.time()
    gen.export_to_parquet(suppliers, str(parquet_path))
    parquet_time = time.time() - start

    # Test CSV export performance
    csv_path = tmp_path / "perf_test.csv"
    start = time.time()
    gen.export_to_csv(suppliers, str(csv_path))
    csv_time = time.time() - start

    print(f"Parquet export: {parquet_time:.3f}s, CSV export: {csv_time:.3f}s")

    # Performance assertions
    assert parquet_time < 5, f"Parquet export too slow: {parquet_time:.2f}s for 2k records"
    assert csv_time < 8, f"CSV export too slow: {csv_time:.2f}s for 2k records"

    # Verify files created
    assert parquet_path.exists()
    assert csv_path.exists()

    # Parquet should be smaller than CSV
    parquet_size = parquet_path.stat().st_size
    csv_size = csv_path.stat().st_size
    print(f"File sizes - Parquet: {parquet_size} bytes, CSV: {csv_size} bytes")
    assert parquet_size < csv_size, f"Parquet ({parquet_size}) should be smaller than CSV ({csv_size})"


# Integration Tests
def test_complete_poc_workflow_integration(tmp_path):
    """Test complete POC workflow: suppliers + parts + dirty data + export"""
    print("\n=== COMPLETE POC WORKFLOW INTEGRATION ===")
    gen = Generator(seed=42, tenant_id="test_poc")

    # Step 1: Generate suppliers
    start_time = time.time()
    suppliers = gen.generate_suppliers(count=500)
    supplier_ids = [s["supplier_id"] for s in suppliers]

    # Step 2: Generate parts
    parts = gen.generate_parts(count=500, supplier_ids=supplier_ids)

    # Step 3: Inject dirty data
    dirty_suppliers = gen.inject_dirty_data(
        suppliers, data_type="suppliers", anomaly_rate=0.06)
    dirty_parts = gen.inject_dirty_data(
        parts, data_type="parts", anomaly_rate=0.06)

    # Step 4: Export both datasets
    supplier_path = tmp_path / "suppliers_poc.parquet"
    parts_path = tmp_path / "parts_poc.parquet"

    gen.export_to_parquet(dirty_suppliers, str(supplier_path))
    gen.export_to_parquet(dirty_parts, str(parts_path))

    total_time = time.time() - start_time
    print(f"Complete POC workflow: {total_time:.3f}s")

    # Performance verification
    assert total_time < 30, f"Complete POC workflow too slow: {total_time:.2f}s"

    # Data integrity verification
    assert len(suppliers) == 500
    assert len(parts) == 500
    assert len(dirty_suppliers) == 500
    assert len(dirty_parts) == 500

    # File verification
    assert supplier_path.exists()
    assert parts_path.exists()

    # Load and verify exported data
    supplier_df = pd.read_parquet(supplier_path)
    parts_df = pd.read_parquet(parts_path)

    assert len(supplier_df) == 500
    assert len(parts_df) == 500

    # Verify tenant consistency
    assert supplier_df["tenant_id"].nunique() == 1
    assert parts_df["tenant_id"].nunique() == 1
    assert supplier_df["tenant_id"].iloc[0] == "test_poc"
    assert parts_df["tenant_id"].iloc[0] == "test_poc"

    # Verify referential integrity in exported data
    exported_supplier_ids = set(supplier_df["supplier_id"].tolist())

    # Count FK violations in exported parts (should only be from dirty data injection)
    fk_violations = 0
    for _, part_row in parts_df.iterrows():
        if part_row["default_supplier_id"] not in exported_supplier_ids:
            fk_violations += 1

    print(f"FK violations from dirty data: {fk_violations}")

    # More realistic expectation: 30 anomalies * ~20% FK violation rate = ~6 violations (allow variance)
    assert 3 <= fk_violations <= 15, f"FK violations {fk_violations} outside expected dirty data range"


def test_full_pipeline_integration(tmp_path, generator):
    """Test complete generation -> dirty injection -> export pipeline"""
    # Generate clean data
    suppliers = generator.generate_suppliers(count=100)
    assert len(suppliers) == 100

    # Inject dirty data
    dirty_suppliers = generator.inject_dirty_data(
        suppliers, data_type="suppliers", anomaly_rate=0.06)
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
def test_data_contract_compliance(generator):
    """Test strict data contract compliance - native Python types only"""
    suppliers = generator.generate_suppliers(count=5)
    supplier_ids = [s["supplier_id"] for s in suppliers]
    parts = generator.generate_parts(count=5, supplier_ids=supplier_ids)

    # Test all suppliers
    for supplier in suppliers:
        # Critical integer fields must be native Python int
        assert type(supplier["lead_time_days_avg"]
                    ) is int, f"lead_time_days_avg is {type(supplier['lead_time_days_avg'])}"
        assert type(supplier["lead_time_days_p95"]
                    ) is int, f"lead_time_days_p95 is {type(supplier['lead_time_days_p95'])}"
        assert type(supplier["defect_rate_ppm"]
                    ) is int, f"defect_rate_ppm is {type(supplier['defect_rate_ppm'])}"
        assert type(supplier["capacity_units_per_week"]
                    ) is int, f"capacity_units_per_week is {type(supplier['capacity_units_per_week'])}"

        # Critical float fields must be native Python float
        assert type(supplier["on_time_delivery_rate"]
                    ) is float, f"on_time_delivery_rate is {type(supplier['on_time_delivery_rate'])}"
        assert type(
            supplier["risk_score"]) is float, f"risk_score is {type(supplier['risk_score'])}"

    # Test all parts
    for part in parts:
        # Critical integer fields must be native Python int
        assert type(part["moq"]) is int, f"moq is {type(part['moq'])}"
        assert type(part["lead_time_days_avg"]
                    ) is int, f"lead_time_days_avg is {type(part['lead_time_days_avg'])}"
        assert type(part["lead_time_days_p95"]
                    ) is int, f"lead_time_days_p95 is {type(part['lead_time_days_p95'])}"

        # Critical float fields must be native Python float
        assert type(part["unit_cost"]
                    ) is float, f"unit_cost is {type(part['unit_cost'])}"

        # String fields must be native Python str
        assert type(part["category"]
                    ) is str, f"category is {type(part['category'])}"
        assert type(part["lifecycle_status"]
                    ) is str, f"lifecycle_status is {type(part['lifecycle_status'])}"
        assert type(part["uom"]) is str, f"uom is {type(part['uom'])}"
        assert type(
            part["quality_grade"]) is str, f"quality_grade is {type(part['quality_grade'])}"


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
        assert len(
            supplier_id) == 26, f"ULID length should be 26, got {len(supplier_id)}"

        # Should only contain valid ULID characters (Crockford's Base32)
        valid_chars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        assert all(
            c in valid_chars for c in supplier_id), f"Invalid ULID characters in {supplier_id}"


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


def test_memory_usage_reasonable():
    """Test memory usage doesn't grow excessively with data size"""
    try:
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        gen = Generator(seed=42)

        # Generate progressively larger datasets
        for size in [1000, 2000, 4000]:
            suppliers = gen.generate_suppliers(count=size)
            supplier_ids = [s["supplier_id"] for s in suppliers]
            parts = gen.generate_parts(count=size, supplier_ids=supplier_ids)

            current_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_growth = current_memory - initial_memory

            print(f"Memory usage for {size} records: {memory_growth:.1f}MB")

            # Memory growth should be reasonable (< 200MB for 4k records)
            assert memory_growth < 200, f"Memory usage too high: {memory_growth:.1f}MB for {size} records"

            # Clean up for next iteration
            del suppliers, parts, supplier_ids

    except ImportError:
        pytest.skip("psutil not available for memory testing")


# Large Scale Test (commented out for regular testing)
# Uncomment and run manually when needed
"""
def test_large_scale_generation():
    '''Test full-scale generation (30k records) - run manually'''
    print("\n=== LARGE SCALE TEST (30K RECORDS) ===")
    gen = Generator(seed=42)
    
    start = time.time()
    suppliers = gen.generate_suppliers(count=30000)
    supplier_time = time.time() - start
    
    supplier_ids = [s["supplier_id"] for s in suppliers]
    
    start = time.time()
    parts = gen.generate_parts(count=30000, supplier_ids=supplier_ids)
    parts_time = time.time() - start
    
    total_time = supplier_time + parts_time
    
    print(f"30k suppliers: {supplier_time:.1f}s")
    print(f"30k parts: {parts_time:.1f}s") 
    print(f"Total: {total_time:.1f}s")
    
    # Should complete in reasonable time (adjust threshold as needed)
    assert total_time < 300, f"30k generation took too long: {total_time:.1f}s"
    assert len(suppliers) == 30000
    assert len(parts) == 30000
    
    # Test data quality at scale
    countries = [s["country"] for s in suppliers]
    cn_pct = countries.count("CN") / len(countries)
    assert 0.15 <= cn_pct <= 0.30, f"CN percentage {cn_pct:.3f} outside expected range"
    
    # Test referential integrity at scale
    supplier_id_set = set(supplier_ids)
    orphan_count = 0
    for part in parts:
        if part["default_supplier_id"] not in supplier_id_set:
            orphan_count += 1
    
    assert orphan_count == 0, f"Found {orphan_count} orphan FKs at scale"
"""
