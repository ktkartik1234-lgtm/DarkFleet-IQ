"""
DarkFleet-IQ: End-to-End Maritime Analytics Pipeline Orchestrator
==================================================================
Executes the full forensic stack in a single automated command:
1. Synthetic Telemetry & Parquet Ingestion (generate_ais_data.py)
2. dbt Core Transformation & Quality Testing (dbt run & test / build_dbt_pipeline.py)
3. Polars & SciPy Statistical Forensics Engine (statistical_forensics.py)
4. GenAI Autonomous Sanctions Compliance Briefing (ai_sanctions_briefing.py)
"""

import os
import sys
import subprocess
import time

# Ensure UTF-8 output across Windows consoles
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis")
DBT_DIR = os.path.join(BASE_DIR, "dbt_darkfleet")

def print_banner(text):
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")

def run_step(description, command, cwd=BASE_DIR, raise_on_error=True):
    print_banner(description)
    start_time = time.time()
    if isinstance(command, list):
        res = subprocess.run(command, cwd=cwd)
    else:
        res = subprocess.run(command, cwd=cwd, shell=True)
    elapsed = time.time() - start_time
    if res.returncode != 0:
        if raise_on_error:
            print(f"\n[ERROR] Step failed with return code {res.returncode}: {description}")
            sys.exit(res.returncode)
        else:
            raise RuntimeError(f"Command '{command}' failed with exit code {res.returncode}")
    print(f"\n[SUCCESS] Completed in {elapsed:.2f}s.")
    return res.returncode

def main():
    total_start = time.time()
    print("================================================================================")
    print("      🛰️ DarkFleet-IQ | Autonomous Maritime Satellite Telemetry Forensics       ")
    print("================================================================================")
    print(f"Working Directory: {BASE_DIR}")
    
    # Step 1: Generate Telemetry & Raw Parquet Lakehouse
    run_step(
        "STEP 1/4: Generating AIS Satellite Telemetry & Ingesting Parquet Lakehouse",
        [sys.executable, os.path.join(SCRIPTS_DIR, "generate_ais_data.py")]
    )
    
    # Step 2: dbt Core Transformations & Data Tests
    # Try native dbt first; fallback to internal orchestrator if needed
    try:
        run_step(
            "STEP 2/4 (A): Executing dbt Core Transformation Pipeline",
            "dbt run --profiles-dir .",
            cwd=DBT_DIR,
            raise_on_error=False
        )
        run_step(
            "STEP 2/4 (B): Running dbt Data Quality & Integrity Test Suite",
            "dbt test --profiles-dir .",
            cwd=DBT_DIR,
            raise_on_error=False
        )
    except Exception as e:
        print(f"\n[WARN] Native dbt CLI failed ({e}). Falling back to DuckDB dbt runner...")
        run_step(
            "STEP 2/4: Executing Lakehouse Transformation Pipeline & Tests",
            [sys.executable, os.path.join(SCRIPTS_DIR, "build_dbt_pipeline.py")],
            raise_on_error=True
        )
        
    # Step 3: Statistical Forensics Suite (Polars & SciPy)
    run_step(
        "STEP 3/4: Executing Polars & SciPy Statistical Forensics Engine",
        [sys.executable, os.path.join(ANALYSIS_DIR, "statistical_forensics.py")]
    )
    
    # Step 4: GenAI Sanctions Intelligence Briefing Generator
    run_step(
        "STEP 4/4: Generating Autonomous Maritime Sanctions Executive Briefing",
        [sys.executable, os.path.join(SCRIPTS_DIR, "ai_sanctions_briefing.py")]
    )
    
    total_elapsed = time.time() - total_start
    print_banner(f"🎉 DarkFleet-IQ Pipeline Completed Successfully in {total_elapsed:.2f}s!")
    print("Key Generated Deliverables:")
    print(f" 1. DuckDB Lakehouse:    {os.path.join(BASE_DIR, 'data', 'darkfleet.duckdb')}")
    print(f" 2. Sanctions Briefing:  {os.path.join(BASE_DIR, 'reports', 'MARITIME_SANCTIONS_EXECUTIVE_BRIEFING.md')}")
    print(f" 3. Statistical JSON:    {os.path.join(BASE_DIR, 'reports', 'statistical_forensics_summary.json')}")
    print(f" 4. Forensics CSV:       {os.path.join(BASE_DIR, 'reports', 'statistical_forensics_table.csv')}")
    print(f" 5. Visualizations:      {os.path.join(BASE_DIR, 'reports', 'fig1_draft_change_distribution.png')}")
    print(f"                         {os.path.join(BASE_DIR, 'reports', 'fig2_gap_duration_by_zone.png')}")
    print(f"                         {os.path.join(BASE_DIR, 'reports', 'fig3_vessel_risk_matrix.png')}")
    print("================================================================================")

if __name__ == "__main__":
    main()
