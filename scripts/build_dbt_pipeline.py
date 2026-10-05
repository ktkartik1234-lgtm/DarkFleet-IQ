"""
DarkFleet-IQ: dbt Transformation Orchestrator & Test Runner
============================================================
Compiles and executes the modular dbt analytical SQL suite against DuckDB:
1. Staging Layer: cleans and types raw vessel registry & telemetry pings.
2. Intermediate Layer: executes window functions (LAG) for temporal gap detection & Haversine math.
3. Marts Layer: materializes forensic dark events, dimensional zones, and composite vessel risk indices.
4. Data Quality & Test Suite: validates not_null, unique, and accepted_values constraints.
"""

import os
import re
import time
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "darkfleet.duckdb")
MODELS_DIR = os.path.join(BASE_DIR, "dbt_darkfleet", "models")

MODEL_EXECUTION_GRAPH = [
    # 1. Staging Layer
    {
        "name": "stg_vessels",
        "layer": "staging",
        "materialization": "VIEW",
        "file": os.path.join(MODELS_DIR, "staging", "stg_vessels.sql")
    },
    {
        "name": "stg_ais_pings",
        "layer": "staging",
        "materialization": "VIEW",
        "file": os.path.join(MODELS_DIR, "staging", "stg_ais_pings.sql")
    },
    {
        "name": "stg_high_risk_zones",
        "layer": "staging",
        "materialization": "VIEW",
        "file": os.path.join(MODELS_DIR, "staging", "stg_high_risk_zones.sql")
    },
    # 2. Intermediate Layer
    {
        "name": "int_ais_gap_analysis",
        "layer": "intermediate",
        "materialization": "TABLE",
        "file": os.path.join(MODELS_DIR, "intermediate", "int_ais_gap_analysis.sql")
    },
    # 3. Marts Layer
    {
        "name": "dim_high_risk_zones",
        "layer": "marts",
        "materialization": "TABLE",
        "file": os.path.join(MODELS_DIR, "marts", "dim_high_risk_zones.sql")
    },
    {
        "name": "fct_dark_events",
        "layer": "marts",
        "materialization": "TABLE",
        "file": os.path.join(MODELS_DIR, "marts", "fct_dark_events.sql")
    },
    {
        "name": "fct_vessel_risk_summary",
        "layer": "marts",
        "materialization": "TABLE",
        "file": os.path.join(MODELS_DIR, "marts", "fct_vessel_risk_summary.sql")
    }
]

# Map model name to target schema.table
MODEL_REF_MAP = {
    "stg_vessels": "staging.stg_vessels",
    "stg_ais_pings": "staging.stg_ais_pings",
    "stg_high_risk_zones": "staging.stg_high_risk_zones",
    "int_ais_gap_analysis": "intermediate.int_ais_gap_analysis",
    "dim_high_risk_zones": "marts.dim_high_risk_zones",
    "fct_dark_events": "marts.fct_dark_events",
    "fct_vessel_risk_summary": "marts.fct_vessel_risk_summary"
}

def compile_dbt_sql(sql_content):
    """Replaces Jinja {{ ref('...') }} with DuckDB schema-qualified table paths."""
    def ref_replace(match):
        ref_name = match.group(1).strip().strip("'\"")
        return MODEL_REF_MAP.get(ref_name, ref_name)
        
    compiled = re.sub(r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", ref_replace, sql_content)
    return compiled

def run_dbt_suite():
    print("================================================================================")
    print("           DarkFleet-IQ | dbt Analytical Transformation Pipeline                ")
    print("================================================================================")
    print(f"[dbt-runner] Target Lakehouse: {DB_PATH}")
    
    con = duckdb.connect(DB_PATH)
    
    # Ensure schemas exist
    con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    con.execute("CREATE SCHEMA IF NOT EXISTS intermediate;")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    
    total_start = time.time()
    
    # 1. Execute Models
    for step in MODEL_EXECUTION_GRAPH:
        name = step["name"]
        layer = step["layer"]
        mat = step["materialization"]
        path = step["file"]
        
        with open(path, "r", encoding="utf-8") as f:
            raw_sql = f.read()
            
        compiled_sql = compile_dbt_sql(raw_sql)
        target_name = f"{layer}.{name}"
        
        start_t = time.time()
        ddl = f"CREATE OR REPLACE {mat} {target_name} AS \n{compiled_sql}"
        con.execute(ddl)
        elapsed = time.time() - start_t
        
        # Count rows
        row_cnt = con.execute(f"SELECT COUNT(*) FROM {target_name}").fetchone()[0]
        print(f" [OK] {layer.upper():<12} | {target_name:<34} | {mat:<5} | {row_cnt:>7,} rows | {elapsed:.2f}s")
        
    print("-" * 80)
    print(" Running Data Quality & Forensic Integrity Test Suite...")
    print("-" * 80)
    
    # 2. Run Test Assertions
    tests = [
        ("stg_vessels.vessel_id NOT NULL", "SELECT COUNT(*) FROM staging.stg_vessels WHERE vessel_id IS NULL"),
        ("stg_vessels.mmsi UNIQUE", "SELECT COUNT(*) - COUNT(DISTINCT mmsi) FROM staging.stg_vessels"),
        ("stg_vessels.flag_risk_category ACCEPTED_VALUES", 
         "SELECT COUNT(*) FROM staging.stg_vessels WHERE flag_risk_category NOT IN ('HIGH_RISK_SANCTION_FLAG', 'FLAG_OF_CONVENIENCE', 'STANDARD_REGISTRY')"),
        ("stg_ais_pings.ping_id UNIQUE", "SELECT COUNT(*) - COUNT(DISTINCT ping_id) FROM staging.stg_ais_pings"),
        ("int_ais_gap_analysis.ping_id UNIQUE", "SELECT COUNT(*) - COUNT(DISTINCT ping_id) FROM intermediate.int_ais_gap_analysis"),
        ("int_ais_gap_analysis.gap_hours NOT NULL", "SELECT COUNT(*) FROM intermediate.int_ais_gap_analysis WHERE gap_hours IS NULL"),
        ("fct_dark_events.event_id UNIQUE", "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM marts.fct_dark_events"),
        ("fct_dark_events.ping_id UNIQUE", "SELECT COUNT(*) - COUNT(DISTINCT ping_id) FROM marts.fct_dark_events"),
        ("fct_dark_events.gap_hours >= 12.0", "SELECT COUNT(*) FROM marts.fct_dark_events WHERE gap_hours < 12.0"),
        ("fct_dark_events.event_typology ACCEPTED_VALUES", 
         "SELECT COUNT(*) FROM marts.fct_dark_events WHERE event_typology NOT IN ('STS_CARGO_DISCHARGE_OFFLOAD', 'STS_CARGO_LOAD_TRANSFER', 'SPOOFING_OR_SUSPECT_CHOKEPOINT_BLACKOUT', 'ROUTINE_TRANSPONDER_LOSS')"),
        ("fct_vessel_risk_summary.mmsi UNIQUE", "SELECT COUNT(*) - COUNT(DISTINCT mmsi) FROM marts.fct_vessel_risk_summary"),
        ("fct_vessel_risk_summary.risk_tier ACCEPTED_VALUES", 
         "SELECT COUNT(*) FROM marts.fct_vessel_risk_summary WHERE risk_tier NOT IN ('CRITICAL_SANCTION_RISK', 'HIGH_SUSPICION', 'MODERATE_WATCHLIST', 'LOW_COMPLIANCE_RISK')")
    ]
    
    passed_tests = 0
    for test_name, query in tests:
        res = con.execute(query).fetchone()[0]
        if res == 0:
            print(f" [PASS] {test_name:<55} (0 failures)")
            passed_tests += 1
        else:
            print(f" [FAIL] {test_name:<55} ({res} violating records)")
            
    print("-" * 80)
    total_elapsed = time.time() - total_start
    print(f"Finished dbt transformation suite in {total_elapsed:.2f}s! ({passed_tests}/{len(tests)} tests passed)")
    print("================================================================================")
    
    con.close()
    if passed_tests < len(tests):
        import sys
        sys.exit(1)

if __name__ == "__main__":
    run_dbt_suite()
