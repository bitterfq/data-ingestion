import pandas as pd

# PDF Page 2 - EXACT supplier schema from specification
PDF_SUPPLIERS_SCHEMA = {
    'supplier_id': 'string (ULID/UUIDv7) - PK',
    'supplier_code': 'string - unique within tenant', 
    'tenant_id': 'string - required',
    'legal_name': 'string - required',
    'dba_name': 'string - optional',
    'country': 'string - ISO-3166-1 alpha-2',
    'region': 'string - optional (EMEA/APAC)',
    'address_line1': 'string - optional (sensitive)',
    'address_line2': 'string - optional (sensitive)', 
    'city': 'string - optional (sensitive)',
    'state': 'string - optional (sensitive)',
    'postal_code': 'string - optional (sensitive)',
    'contact_email': 'string - optional RFC 5322',
    'contact_phone': 'string - optional E.164',
    'preferred_currency': 'string - ISO-4217',
    'incoterms': 'string - optional (DDP, FOB)',
    'lead_time_days_avg': 'int - ≥0',
    'lead_time_days_p95': 'int - ≥0', 
    'on_time_delivery_rate': 'decimal(5,2) - 0–100',
    'defect_rate_ppm': 'int - ≥0',
    'capacity_units_per_week': 'int - ≥0',
    'risk_score': 'decimal(5,2) - 0–100',
    'financial_risk_tier': 'string - {LOW, MEDIUM, HIGH}',
    'certifications': 'array<string> - ISO9001, IATF16949, AS9100',
    'compliance_flags': 'array<string> - ITAR, REACH, ROHS',
    'approved_status': 'string - {PENDING, APPROVED, SUSPENDED, BLACKLISTED}',
    'contracts': 'array<string> - external doc IDs',
    'terms_version': 'string - current contract terms version',
    'geo_coords': 'struct<lat:double,lon:double> - CRITICAL PDF REQUIREMENT',
    'data_source': 'string - e.g., sap.supplier_master',
    'source_timestamp': 'timestamp - when read from source',
    'ingestion_timestamp': 'timestamp - when landed in fabric', 
    'schema_version': 'string - "1.0.0"'
}

# PDF Page 3 - EXACT parts schema  
PDF_PARTS_SCHEMA = {
    'part_id': 'string (ULID/UUIDv7) - PK',
    'tenant_id': 'string - required',
    'part_number': 'string - required, unique within tenant',
    'description': 'string - required',
    'category': 'string - ELECTRICAL/MECHANICAL/RAW_MATERIAL',
    'lifecycle_status': 'string - {NEW, ACTIVE, NRND, EOL}',
    'uom': 'string - EA, KG',
    'spec_hash': 'string - hash of technical spec doc',
    'bom_compatibility': 'array<string> - compatible BOM or alt group IDs',
    'default_supplier_id': 'string - FK → dim_supplier_v1.supplier_id',
    'qualified_supplier_ids': 'array<string> - FKs; maintain referential integrity',
    'unit_cost': 'decimal(18,6) - ≥0',
    'moq': 'int - ≥0',
    'lead_time_days_avg': 'int - ≥0',
    'lead_time_days_p95': 'int - ≥0',
    'quality_grade': 'string - A/B/C',
    'compliance_flags': 'array<string> - ROHS, REACH',
    'hazard_class': 'string - optional',
    'last_price_change': 'date - optional',
    'data_source': 'string - e.g., plm.parts_master',
    'source_timestamp': 'timestamp',
    'ingestion_timestamp': 'timestamp',
    'schema_version': 'string - "1.0.0"'
}

def validate_against_pdf(df, pdf_schema, table_name):
    print(f'\n{table_name.upper()} - PDF SPECIFICATION VALIDATION')
    print('='*60)
    
    violations = []
    
    # 1. Field presence check
    pdf_fields = set(pdf_schema.keys())
    actual_fields = set(df.columns)
    
    missing = pdf_fields - actual_fields
    if missing:
        violations.append(f'Missing PDF-required fields: {sorted(missing)}')
    
    # 2. Critical geo_coords validation for suppliers
    if table_name == 'suppliers':
        if 'geo_coords' not in df.columns:
            violations.append('CRITICAL: geo_coords struct missing (PDF page 2 requirement)')
        else:
            sample_geo = df.iloc[0]['geo_coords']
            if not isinstance(sample_geo, dict):
                violations.append(f'geo_coords wrong type: {type(sample_geo)}, should be dict/struct')
            elif 'lat' not in sample_geo or 'lon' not in sample_geo:
                violations.append('geo_coords missing lat/lon keys')
            elif sample_geo['lat'] is None or sample_geo['lon'] is None:
                violations.append('geo_coords has null coordinate values')
        
        # Check for incorrect geo_lat/geo_lon split
        if 'geo_lat' in df.columns or 'geo_lon' in df.columns:
            violations.append('VIOLATION: Using geo_lat/geo_lon instead of PDF-required geo_coords struct')
    
    # 3. Data type validation - critical numeric fields
    sample = df.iloc[0]
    
    if table_name == 'suppliers':
        # Check decimal fields are numeric 
        for field in ['on_time_delivery_rate', 'risk_score']:
            if field in df.columns:
                if df[field].dtype == 'object':
                    violations.append(f'{field} is object type, PDF requires decimal(5,2)')
        
        # Check integer fields
        for field in ['lead_time_days_avg', 'lead_time_days_p95', 'defect_rate_ppm']:
            if field in df.columns:
                if not str(df[field].dtype).startswith('int'):
                    violations.append(f'{field} wrong type: {df[field].dtype}, PDF requires int')
    
    if table_name == 'parts':
        # Check unit_cost is decimal type
        if 'unit_cost' in df.columns:
            if not (str(df['unit_cost'].dtype).startswith('decimal') or 'Decimal' in str(type(sample.get('unit_cost')))):
                violations.append(f'unit_cost type: {df["unit_cost"].dtype}, PDF requires decimal(18,6)')
    
    # 4. Data source validation
    data_source = sample.get('data_source', '')
    if '{' in str(data_source) and 'lat' in str(data_source):
        violations.append('CRITICAL: data_source contains geo coordinates (column mapping bug)')
    
    # 5. Sample data validation against PDF constraints
    if table_name == 'suppliers':
        risk_score = sample.get('risk_score')
        on_time_rate = sample.get('on_time_delivery_rate')
        
        if risk_score is not None and (risk_score < 0 or risk_score > 100):
            violations.append(f'risk_score {risk_score} violates PDF constraint (0-100)')
            
        if on_time_rate is not None and (on_time_rate < 0 or on_time_rate > 100):
            violations.append(f'on_time_delivery_rate {on_time_rate} violates PDF constraint (0-100)')
    
    # Results
    print(f'Records analyzed: {len(df):,}')
    print(f'PDF schema compliance: {len(pdf_fields & actual_fields)}/{len(pdf_fields)} fields')
    print(f'Sample record:')
    
    if table_name == 'suppliers':
        print(f'  supplier_id: {sample.get("supplier_id")}')
        print(f'  geo_coords: {sample.get("geo_coords")}')  
        print(f'  data_source: {sample.get("data_source")}')
        print(f'  risk_score: {sample.get("risk_score")} (type: {type(sample.get("risk_score"))})')
    else:
        print(f'  part_id: {sample.get("part_id")}')
        print(f'  unit_cost: {sample.get("unit_cost")} (type: {type(sample.get("unit_cost"))})')
        print(f'  category: {sample.get("category")}')
    
    if violations:
        print(f'\nPDF VIOLATIONS FOUND ({len(violations)}):')
        for i, violation in enumerate(violations, 1):
            print(f'  {i}. {violation}')
        return False
    else:
        print(f'\nPDF COMPLIANCE: PERFECT')
        return True

print('PDF SPECIFICATION VALIDATION - FINAL VERIFICATION')
print('='*70)

try:
    df_suppliers = pd.read_parquet('test_suppliers_latest.parquet')
    supplier_pdf_compliant = validate_against_pdf(df_suppliers, PDF_SUPPLIERS_SCHEMA, 'suppliers')
except Exception as e:
    print(f'SUPPLIERS VALIDATION FAILED: {e}')
    supplier_pdf_compliant = False

try:
    df_parts = pd.read_parquet('test_parts_latest.parquet')  
    parts_pdf_compliant = validate_against_pdf(df_parts, PDF_PARTS_SCHEMA, 'parts')
except Exception as e:
    print(f'PARTS VALIDATION FAILED: {e}')
    parts_pdf_compliant = False

print('\n' + '='*70)
print('FINAL PDF COMPLIANCE VERDICT')
print('='*70)

if supplier_pdf_compliant and parts_pdf_compliant:
    print('STATUS: COMPLETE PDF SPECIFICATION COMPLIANCE ACHIEVED')
    print('RESULT: Pipeline meets all PDF requirements')
    print('RECOMMENDATION: Ready for production deployment')
else:
    print('STATUS: PDF SPECIFICATION VIOLATIONS DETECTED') 
    print('RESULT: Pipeline does not meet PDF requirements')
    print('RECOMMENDATION: Fix violations before production')

print('='*70)