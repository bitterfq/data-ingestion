## Data Generatator Outline

### **Scale & Basic Goals (Section 3.1)**
- 30,000 suppliers and 30,000 parts per tenant (start with one tenant)
- Preserve referential integrity: each part must map to 1+ qualified suppliers
- Simulate real distributions (skew, outliers) & dirty data (to test DQ gates)

### **Supplier Distributions (Section 3.2)**
```
Countries: Zipfian skew (CN, US, DE, MX, IN, VN, PL…)
Risk: bimodal (safe cluster 20–40; risky 60–85)
On-time rate: normal(μ=93, σ=5), cap [60, 100]
Certifications: sample from weighted set (ISO9001=0.7, IATF16949=0.3, AS9100=0.1)
```

### **Part Distributions (Section 3.2)**
```
Categories: ELECTRICAL 35%, MECHANICAL 35%, RAW_MATERIAL 20%, OTHER 10%
Lifecycle: ACTIVE 75%, NEW 10%, NRND 10%, EOL 5%
Unit cost: log-normal (wide tail for specialty parts)
Lead time: category-dependent (RAW_MATERIAL shorter; ELECTRICAL longer)
```

### **Correlations (Section 3.2)**
```
Suppliers with high on_time_delivery_rate → lower risk_score
Parts in ELECTRICAL category → higher lead_time_days_p95
Certifications AS9100 more common for A&D category parts
```

### **Dirty Data (Section 3.3)**
```
Introduce 5–8% anomalies to exercise DQ:
- Missing emails/phones; bogus country codes; out-of-range rates (e.g., 104%)
- Duplicate supplier_code within tenant; whitespace, casing issues
- For parts: invalid qualified_supplier_ids, negative costs, wrong UOM
- Timestamp drifts (future source_timestamp) to test freshness rules
```
