-- Supply Chain Database Schema
-- Creates tables matching your current CSV structure

CREATE TABLE suppliers
(
    supplier_id VARCHAR(26) PRIMARY KEY,
    tenant_id VARCHAR(50) NOT NULL,
    supplier_code VARCHAR(50),
    legal_name VARCHAR(255) NOT NULL,
    dba_name VARCHAR(255),
    country VARCHAR(2),
    region VARCHAR(50),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    preferred_currency VARCHAR(3),
    incoterms VARCHAR(10),
    lead_time_days_avg INTEGER,
    lead_time_days_p95 INTEGER,
    on_time_delivery_rate DECIMAL(5,2),
    defect_rate_ppm INTEGER,
    capacity_units_per_week INTEGER,
    risk_score DECIMAL(5,2),
    financial_risk_tier VARCHAR(10),
    certifications JSONB,
    compliance_flags JSONB,
    approved_status VARCHAR(20),
    contracts JSONB,
    terms_version VARCHAR(20),
    geo_coords JSONB,
    data_source VARCHAR(100),
    source_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_version VARCHAR(10) DEFAULT '1.0.0'
);

CREATE TABLE parts
(
    part_id VARCHAR(26) PRIMARY KEY,
    tenant_id VARCHAR(50) NOT NULL,
    part_number VARCHAR(100) NOT NULL,
    description TEXT,
    category VARCHAR(50),
    lifecycle_status VARCHAR(20),
    uom VARCHAR(10),
    spec_hash VARCHAR(64),
    bom_compatibility JSONB,
    default_supplier_id VARCHAR(26),
    qualified_supplier_ids JSONB,
    unit_cost DECIMAL(18,6),
    moq INTEGER,
    lead_time_days_avg INTEGER,
    lead_time_days_p95 INTEGER,
    quality_grade VARCHAR(1),
    compliance_flags JSONB,
    hazard_class VARCHAR(50),
    last_price_change DATE,
    data_source VARCHAR(100),
    source_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_version VARCHAR(10) DEFAULT '1.0.0',
    FOREIGN KEY (default_supplier_id) REFERENCES suppliers(supplier_id)
);

-- Indexes for performance
CREATE INDEX idx_suppliers_tenant_id ON suppliers(tenant_id);
CREATE INDEX idx_suppliers_approved_status ON suppliers(approved_status);
CREATE INDEX idx_parts_tenant_id ON parts(tenant_id);
CREATE INDEX idx_parts_category ON parts(category);
CREATE INDEX idx_parts_default_supplier ON parts(default_supplier_id);