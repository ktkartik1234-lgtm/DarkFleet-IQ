"""
DarkFleet-IQ: Geopolitical AIS Maritime Satellite Telemetry & Dark-Zone Transponder Forensics
Data Generation Engine
==============================================================================================
Generates high-fidelity synthetic AIS satellite telemetry simulating both standard commercial
shipping routes and illicit 'Dark Fleet' (Shadow Fleet) operations across geopolitical choke points:
- Persian Gulf / Strait of Hormuz (OFAC Sanctions Evasion)
- Malacca & Singapore Strait / Riau Archipelago (Illicit STS Blending Hub)
- Black Sea / Kerch Strait (Russian Crude Price-Cap Evasion)
- Gulf of Oman / Fujairah Offshore (Covert STS & Bunkering Zone)

Outputs:
  - data/raw/vessels.parquet
  - data/raw/ais_pings.parquet
  - data/raw/high_risk_zones.parquet
  - data/darkfleet.duckdb (DuckDB Lakehouse database with raw schema)
"""

import os
import sys
import math
import random
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# Set deterministic seed for complete reproducibility
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# Define Project Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
DB_PATH = os.path.join(BASE_DIR, "data", "darkfleet.duckdb")

os.makedirs(DATA_RAW_DIR, exist_ok=True)

print(f"[DarkFleet-IQ DataGen] Initializing pipeline in: {BASE_DIR}")

# -------------------------------------------------------------------------
# 1. High-Risk Maritime Geopolitical Zones
# -------------------------------------------------------------------------
HIGH_RISK_ZONES = [
    {
        "zone_id": "ZONE_HORMUZ",
        "zone_name": "Strait of Hormuz & Persian Gulf",
        "sanctions_regime": "OFAC Iranian Petroleum Sanctions",
        "primary_threat": "Covert crude loading & transponder manipulation",
        "risk_level": "CRITICAL",
        "min_lat": 24.0,
        "max_lat": 27.5,
        "min_lon": 52.0,
        "max_lon": 57.5,
        "center_lat": 25.8,
        "center_lon": 54.8
    },
    {
        "zone_id": "ZONE_MALACCA",
        "zone_name": "Malacca Strait & Riau Archipelago",
        "sanctions_regime": "Mid-Sea STS Blending & Cargo Obfuscation",
        "primary_threat": "Ship-to-Ship (STS) crude transfers & false origin declaration",
        "risk_level": "CRITICAL",
        "min_lat": 1.0,
        "max_lat": 3.8,
        "min_lon": 102.0,
        "max_lon": 105.5,
        "center_lat": 2.2,
        "center_lon": 103.8
    },
    {
        "zone_id": "ZONE_BLACK_SEA",
        "zone_name": "Black Sea & Kerch Strait Approach",
        "sanctions_regime": "G7 / EU Russian Crude Price Cap Evasion",
        "primary_threat": "Shadow tanker fleet oil exports via unmonitored STS",
        "risk_level": "HIGH",
        "min_lat": 43.5,
        "max_lat": 45.8,
        "min_lon": 35.0,
        "max_lon": 39.5,
        "center_lat": 44.6,
        "center_lon": 37.2
    },
    {
        "zone_id": "ZONE_OMAN_FUJAIRAH",
        "zone_name": "Gulf of Oman Offshore Anchorage",
        "sanctions_regime": "Middle East Crude Arbitrage & Bunkering Evasion",
        "primary_threat": "Covert STS transfers outside territorial limits",
        "risk_level": "HIGH",
        "min_lat": 23.5,
        "max_lat": 26.0,
        "min_lon": 56.5,
        "max_lon": 59.8,
        "center_lat": 24.8,
        "center_lon": 58.1
    }
]

# -------------------------------------------------------------------------
# 2. Vessel Fleet Generation Specs
# -------------------------------------------------------------------------
HIGH_RISK_FLAGS = ["Gabon", "Cook Islands", "Cameroon", "Palau", "Eswatini", "Iran", "Russia"]
CONVENIENCE_FLAGS = ["Panama", "Liberia", "Marshall Islands", "Malta", "Bahamas"]
STANDARD_FLAGS = ["Singapore", "Greece", "Norway", "Japan", "United Kingdom", "Denmark", "Germany"]

TANKER_NAMES_HIGH_RISK = [
    "NEPTUNE GLORY", "TITAN PROSPECTOR", "GULF PHOENIX", "VALIANT SHADOW", "BLACK PEARL",
    "CASPIAN VOYAGER", "HORIZON LEADER", "ORION TRADER", "PHOENIX MARINER", "SEA OBSIDIAN",
    "STELLA MARIS II", "NORDIC TRADER", "AQUILA VOYAGER", "SOLARIS STAR", "EMPEROR SPIRIT",
    "CRONOS VI", "AURORA SUN", "PERSIAN PRIDE", "SIRIUS DELTA", "ZEUS CARRIER",
    "LIBERTY BREEZE", "SILVER FALCON", "VOLGA ENTERPRISE", "KALLISTO", "NORDIC GLORY",
    "BALTIC PIONEER", "PACIFIC EMERALD", "GOLDEN OASIS", "ALBATROSS PRIME", "MYSTIC LEADER"
]

NORMAL_VESSEL_NAMES = [
    "MAERSK COPENHAGEN", "MSC GENEVA", "CMA CGM ANTOINE", "COSCO PRIDE", "EVER GIVEN II",
    "HAPAG HAMBURG", "ONE EAGLE", "FRONT HERCULES", "EURONAV MONACO", "TEEKAY SPIRIT",
    "NORDIC LIGHT", "STENA SUPREME", "BW PIONEER", "MINERVA OCEAN", "MARAN GAS APOLLO",
    "GASLOG SALEM", "DHT TIGER", "OVERSEAS SANTORINI", "BERGE BULKER", "VALE BRASIL",
    "STAR CLIPPER", "NYK CONSTELLATION", "MOL TRIUMPH", "K-LINE CHALLENGER", "PACIFIC RUBY",
    "OOCL ROTTERDAM", "YANG MING UNITY", "HYUNDAI BRAVERY", "ZIM ANTWERP", "PACIFIC HORIZON",
    "NORTHERN VOYAGER", "GLOBAL EMPRESS", "SEASPAN DIAMOND", "ATLANTIC VENTURE", "OCEAN PEAK",
    "ASIAN PROGRESS", "MEDITERRANEAN WIND", "BALTIC SEA SPIRIT", "CARIBBEAN HARBOR", "APL OAKLAND",
    "NORDIC DISCOVERY", "ARABIAN SKY", "TAIWAN FORTUNE", "SINGAPORE WAVE", "DUBAI PEARL"
]

def generate_vessel_registry(num_dark_vessels=30, num_normal_vessels=70):
    """
    Creates a calibrated registry of maritime vessels with realistic metadata,
    differentiating normal commercial operators from shadow/dark fleet candidates.
    """
    vessels = []
    
    # 1. Dark Fleet Tankers (Heavy crude tankers, shadow flag concentration)
    for i in range(num_dark_vessels):
        imo = 9100000 + i * 137 + random.randint(10, 99)
        mmsi = 500000000 + i * 987 + random.randint(100, 999)
        name = TANKER_NAMES_HIGH_RISK[i % len(TANKER_NAMES_HIGH_RISK)]
        if i >= len(TANKER_NAMES_HIGH_RISK):
            name = f"{name} {i // len(TANKER_NAMES_HIGH_RISK) + 1}"
            
        vessel_type = random.choice(["Crude Oil Tanker", "Crude Oil Tanker", "Product Tanker", "Chemical Tanker"])
        
        # High likelihood of flagged under shadow or convenience registries
        flag_prob = random.random()
        if flag_prob < 0.60:
            flag = random.choice(HIGH_RISK_FLAGS)
            flag_risk = "HIGH_RISK_SANCTION_FLAG"
        elif flag_prob < 0.90:
            flag = random.choice(CONVENIENCE_FLAGS)
            flag_risk = "FLAG_OF_CONVENIENCE"
        else:
            flag = random.choice(STANDARD_FLAGS)
            flag_risk = "STANDARD_REGISTRY"
            
        # Heavy tankers: VLCC (200k-320k DWT), Suezmax (120k-160k DWT), Aframax (80k-120k DWT)
        dwt_tier = random.choice(["VLCC", "Suezmax", "Aframax"])
        if dwt_tier == "VLCC":
            dwt = random.randint(250000, 315000)
            max_draft = round(random.uniform(19.0, 21.5), 2)
            ballast_draft = round(random.uniform(8.5, 9.8), 2)
        elif dwt_tier == "Suezmax":
            dwt = random.randint(130000, 160000)
            max_draft = round(random.uniform(15.0, 17.2), 2)
            ballast_draft = round(random.uniform(7.8, 8.8), 2)
        else: # Aframax
            dwt = random.randint(85000, 118000)
            max_draft = round(random.uniform(13.5, 15.2), 2)
            ballast_draft = round(random.uniform(7.0, 8.2), 2)
            
        build_year = random.randint(1998, 2012) # Shadow fleet skews older (>15-20 years old)
        
        vessels.append({
            "vessel_id": f"VESSEL_{i+1:04d}",
            "imo_number": int(imo),
            "mmsi": int(mmsi),
            "vessel_name": name,
            "vessel_type": vessel_type,
            "vessel_class": dwt_tier,
            "flag_country": flag,
            "flag_risk_category": flag_risk,
            "deadweight_tonnage": dwt,
            "max_design_draft_m": max_draft,
            "ballast_draft_m": ballast_draft,
            "build_year": build_year,
            "vessel_age_years": 2026 - build_year,
            "is_dark_fleet_suspect": True
        })
        
    # 2. Normal Commercial Fleet (Diverse types, modern, reputable flags)
    for j in range(num_normal_vessels):
        idx = num_dark_vessels + j
        imo = 9400000 + j * 241 + random.randint(10, 99)
        mmsi = 200000000 + j * 1234 + random.randint(100, 999)
        name = NORMAL_VESSEL_NAMES[j % len(NORMAL_VESSEL_NAMES)]
        if j >= len(NORMAL_VESSEL_NAMES):
            name = f"{name} {j // len(NORMAL_VESSEL_NAMES) + 1}"
            
        vessel_type = random.choice([
            "Container Ship", "Container Ship", "Bulk Carrier", "Crude Oil Tanker", 
            "Product Tanker", "LNG Carrier", "Chemical Tanker"
        ])
        
        flag_prob = random.random()
        if flag_prob < 0.65:
            flag = random.choice(STANDARD_FLAGS)
            flag_risk = "STANDARD_REGISTRY"
        elif flag_prob < 0.95:
            flag = random.choice(CONVENIENCE_FLAGS)
            flag_risk = "FLAG_OF_CONVENIENCE"
        else:
            flag = random.choice(HIGH_RISK_FLAGS)
            flag_risk = "HIGH_RISK_SANCTION_FLAG"
            
        if vessel_type in ["Crude Oil Tanker", "LNG Carrier"]:
            dwt_tier = random.choice(["VLCC", "Suezmax", "Aframax"])
            dwt = random.randint(90000, 300000)
            max_draft = round(random.uniform(14.0, 20.5), 2)
            ballast_draft = round(random.uniform(7.5, 9.5), 2)
        elif vessel_type == "Container Ship":
            dwt_tier = "Ultra Large Container"
            dwt = random.randint(120000, 220000)
            max_draft = round(random.uniform(14.0, 16.5), 2)
            ballast_draft = round(random.uniform(10.0, 12.0), 2)
        else:
            dwt_tier = "Panamax / Handymax"
            dwt = random.randint(40000, 85000)
            max_draft = round(random.uniform(10.5, 13.5), 2)
            ballast_draft = round(random.uniform(6.5, 7.8), 2)
            
        build_year = random.randint(2013, 2024) # Modern commercial fleet
        
        vessels.append({
            "vessel_id": f"VESSEL_{idx+1:04d}",
            "imo_number": int(imo),
            "mmsi": int(mmsi),
            "vessel_name": name,
            "vessel_type": vessel_type,
            "vessel_class": dwt_tier,
            "flag_country": flag,
            "flag_risk_category": flag_risk,
            "deadweight_tonnage": dwt,
            "max_design_draft_m": max_draft,
            "ballast_draft_m": ballast_draft,
            "build_year": build_year,
            "vessel_age_years": 2026 - build_year,
            "is_dark_fleet_suspect": False
        })
        
    return pd.DataFrame(vessels)

# -------------------------------------------------------------------------
# 3. AIS Telemetry Generation with Injected Forensics
# -------------------------------------------------------------------------
def haversine_step(lat, lon, speed_knots, course_deg, hours):
    """Calculates next lat/lon given speed in knots, heading, and time delta."""
    # 1 knot = 1.852 km/h
    dist_km = speed_knots * 1.852 * hours
    R = 6371.0
    
    rad_lat = math.radians(lat)
    rad_lon = math.radians(lon)
    rad_course = math.radians(course_deg)
    
    new_lat = math.asin(
        math.sin(rad_lat) * math.cos(dist_km / R) +
        math.cos(rad_lat) * math.sin(dist_km / R) * math.cos(rad_course)
    )
    new_lon = rad_lon + math.atan2(
        math.sin(rad_course) * math.sin(dist_km / R) * math.cos(rad_lat),
        math.cos(dist_km / R) - math.sin(rad_lat) * math.sin(new_lat)
    )
    
    return math.degrees(new_lat), math.degrees(new_lon)

def generate_telemetry(vessels_df, sim_days=30):
    """
    Generates 30 days of hourly AIS pings with injected dark transponder events
    and mid-sea STS draft depth shifts.
    """
    start_time = datetime(2026, 8, 1, 0, 0, 0)
    end_time = start_time + timedelta(days=sim_days)
    
    pings = []
    ping_id_counter = 1000001
    
    print(f"[DarkFleet-IQ DataGen] Generating AIS telemetry for {len(vessels_df)} vessels over {sim_days} days...")
    
    for _, vessel in vessels_df.iterrows():
        mmsi = vessel["mmsi"]
        is_dark = vessel["is_dark_fleet_suspect"]
        max_draft = vessel["max_design_draft_m"]
        ballast_draft = vessel["ballast_draft_m"]
        
        # Decide voyage origin & path
        if is_dark:
            # Targets high-risk zone
            assigned_zone = random.choice(HIGH_RISK_ZONES)
            curr_lat = assigned_zone["center_lat"] + random.uniform(-1.0, 1.0)
            curr_lon = assigned_zone["center_lon"] + random.uniform(-1.0, 1.0)
            # Starts fully laden or in ballast
            initially_laden = random.choice([True, False])
            curr_draft = max_draft - round(random.uniform(0.1, 0.5), 2) if initially_laden else ballast_draft + round(random.uniform(0.1, 0.4), 2)
            # Injected dark event timing: 1 or 2 events during the month
            num_events = random.choice([1, 2])
            dark_event_windows = []
            for ev in range(num_events):
                event_day = random.randint(4 + ev * 12, 11 + ev * 12)
                event_start = start_time + timedelta(days=event_day, hours=random.randint(0, 23))
                # Transponder switched off for 14 to 46 hours
                event_duration_hours = random.choice([14, 18, 24, 28, 36, 42, 48])
                event_end = event_start + timedelta(hours=event_duration_hours)
                
                # Determine draft shift direction:
                # If currently laden: offloads crude to another ship (STS) -> draft drops to ballast
                # If in ballast: secretly loads crude -> draft increases to laden
                dark_event_windows.append({
                    "start": event_start,
                    "end": event_end,
                    "duration_hours": event_duration_hours,
                    "target_zone": assigned_zone["zone_name"],
                    "target_zone_id": assigned_zone["zone_id"]
                })
        else:
            # Normal commercial route
            route_type = random.choice(["ASIA_EUROPE", "TRANS_PACIFIC", "PERSIAN_FAR_EAST", "MEDITERRANEAN"])
            if route_type == "ASIA_EUROPE":
                curr_lat = random.uniform(5.0, 12.0)
                curr_lon = random.uniform(80.0, 95.0)
                heading = random.uniform(260, 290)
            elif route_type == "TRANS_PACIFIC":
                curr_lat = random.uniform(25.0, 35.0)
                curr_lon = random.uniform(130.0, 150.0)
                heading = random.uniform(70, 100)
            elif route_type == "PERSIAN_FAR_EAST":
                curr_lat = random.uniform(20.0, 25.0)
                curr_lon = random.uniform(58.0, 68.0)
                heading = random.uniform(110, 140)
            else:
                curr_lat = random.uniform(32.0, 36.0)
                curr_lon = random.uniform(15.0, 28.0)
                heading = random.uniform(80, 110)
                
            initially_laden = random.choice([True, False])
            curr_draft = max_draft - round(random.uniform(0.2, 0.6), 2) if initially_laden else ballast_draft + round(random.uniform(0.1, 0.4), 2)
            dark_event_windows = []
            
            # Normal vessels may have an occasional short benign satellite blackout (1-3 hours max, no draft change)
            if random.random() < 0.25:
                b_day = random.randint(5, 25)
                b_start = start_time + timedelta(days=b_day, hours=random.randint(2, 18))
                b_dur = random.choice([2, 3, 4])
                dark_event_windows.append({
                    "start": b_start,
                    "end": b_start + timedelta(hours=b_dur),
                    "duration_hours": b_dur,
                    "is_benign": True
                })

        # Telemetry loop across the 30-day window
        curr_time = start_time
        course = random.uniform(0, 359)
        
        while curr_time < end_time:
            # Check if current timestamp falls within a dark event blackout window
            in_blackout = False
            active_dark_event = None
            for win in dark_event_windows:
                if win["start"] <= curr_time < win["end"]:
                    in_blackout = True
                    active_dark_event = win
                    break
                    
            if in_blackout:
                # Transponder is OFF! Vessel broadcasts NO AIS telemetry
                # Advance timestamp to the end of the blackout
                curr_time = active_dark_event["end"]
                
                # Perform the illicit or benign state change during blackout
                if is_dark:
                    # Significant mid-sea STS transfer happened during dark window!
                    # Haversine drift: vessel drifts or navigates covertly 20 to 70 km away within choke point
                    drift_course = (course + random.uniform(-40, 40)) % 360
                    covert_speed = random.uniform(1.2, 2.8) # slow speed during STS / covert anchoring
                    curr_lat, curr_lon = haversine_step(curr_lat, curr_lon, covert_speed, drift_course, active_dark_event["duration_hours"])
                    # Ensure re-emergence remains within or adjacent to high risk zone perimeter
                    curr_lat = max(assigned_zone["min_lat"] - 0.1, min(assigned_zone["max_lat"] + 0.1, curr_lat))
                    curr_lon = max(assigned_zone["min_lon"] - 0.1, min(assigned_zone["max_lon"] + 0.1, curr_lon))
                    
                    # Major draft change:
                    if curr_draft > (max_draft + ballast_draft) / 2:
                        # Tanker offloaded crude mid-sea -> drops to ballast
                        curr_draft = ballast_draft + round(random.uniform(-0.3, 0.4), 2)
                    else:
                        # Tanker loaded illicit sanctioned crude mid-sea -> sinks to fully laden
                        curr_draft = max_draft - round(random.uniform(0.1, 0.5), 2)
                else:
                    # Benign gap (e.g. atmospheric interference) -> normal continuous sailing, zero draft delta
                    normal_speed = random.uniform(12.0, 15.0)
                    curr_lat, curr_lon = haversine_step(curr_lat, curr_lon, normal_speed, course, active_dark_event["duration_hours"])
                    # Draft remains virtually identical (slight fuel burn < 0.05m)
                    curr_draft = max(ballast_draft, curr_draft - round(random.uniform(0.01, 0.03), 2))
                    
                continue
                
            # If a dark event is impending in the next 3 hours, maneuver into assigned high-risk zone
            if is_dark:
                for win in dark_event_windows:
                    if timedelta(hours=0) <= (win["start"] - curr_time) <= timedelta(hours=3):
                        curr_lat = assigned_zone["center_lat"] + random.uniform(-0.3, 0.3)
                        curr_lon = assigned_zone["center_lon"] + random.uniform(-0.3, 0.3)
                        break

            # Normal broadcasting condition
            # Cruising speed: 11-16 knots, occasional slowing down in port/anchorage
            speed_knots = round(random.uniform(11.5, 15.2), 1)
            # Course slight wander
            course = (course + random.uniform(-3.0, 3.0)) % 360.0
            heading = int(course + random.uniform(-2, 2)) % 360
            
            # Navigational status: Under way using engine
            nav_status = "Under way using engine"
            
            # Tiny sensor draft variation (normal fuel burn or wave action +/- 0.02m)
            curr_draft = round(curr_draft + random.uniform(-0.02, 0.02), 2)
            curr_draft = max(ballast_draft - 0.5, min(max_draft + 0.5, curr_draft))
            
            pings.append({
                "ping_id": ping_id_counter,
                "mmsi": int(mmsi),
                "timestamp": curr_time,
                "latitude": round(curr_lat, 5),
                "longitude": round(curr_lon, 5),
                "speed_knots": speed_knots,
                "course_over_ground": round(course, 1),
                "heading": heading,
                "draft_depth_meters": curr_draft,
                "navigational_status": nav_status
            })
            ping_id_counter += 1
            
            # Advance step: regular ping interval between 30 and 60 minutes
            step_hours = random.choice([0.5, 0.75, 1.0])
            curr_lat, curr_lon = haversine_step(curr_lat, curr_lon, speed_knots, course, step_hours)
            
            # Boundary sanity checks
            curr_lat = max(-80.0, min(80.0, curr_lat))
            if curr_lon > 180.0: curr_lon -= 360.0
            if curr_lon < -180.0: curr_lon += 360.0
            
            curr_time += timedelta(hours=step_hours)
            
    pings_df = pd.DataFrame(pings)
    return pings_df

# -------------------------------------------------------------------------
# 4. Storage to Compressed Parquet & DuckDB Lakehouse Initialization
# -------------------------------------------------------------------------
def export_lakehouse(vessels_df, pings_df, zones_df):
    """
    Saves datasets to compressed Parquet and loads them directly into DuckDB.
    """
    print("[DarkFleet-IQ DataGen] Writing Apache Parquet partitions...")
    vessels_parquet = os.path.join(DATA_RAW_DIR, "vessels.parquet")
    pings_parquet = os.path.join(DATA_RAW_DIR, "ais_pings.parquet")
    zones_parquet = os.path.join(DATA_RAW_DIR, "high_risk_zones.parquet")
    
    # Save Parquet with snappy compression
    vessels_df.to_parquet(vessels_parquet, compression="snappy", index=False)
    pings_df.to_parquet(pings_parquet, compression="snappy", index=False)
    zones_df.to_parquet(zones_parquet, compression="snappy", index=False)
    
    print(f" -> Saved: {vessels_parquet} ({len(vessels_df):,} rows)")
    print(f" -> Saved: {pings_parquet} ({len(pings_df):,} telemetry pings)")
    print(f" -> Saved: {zones_parquet} ({len(zones_df):,} geopolitical risk zones)")
    
    # Initialize DuckDB Database
    print(f"[DarkFleet-IQ DataGen] Connecting to DuckDB: {DB_PATH}")
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
        except Exception:
            pass
            
    con = duckdb.connect(DB_PATH)
    
    # Create schemas and populate raw tables directly from Parquet files
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute(f"CREATE OR REPLACE TABLE raw.vessels AS SELECT * FROM read_parquet('{vessels_parquet.replace(os.sep, '/')}');")
    con.execute(f"CREATE OR REPLACE TABLE raw.ais_pings AS SELECT * FROM read_parquet('{pings_parquet.replace(os.sep, '/')}');")
    con.execute(f"CREATE OR REPLACE TABLE raw.high_risk_zones AS SELECT * FROM read_parquet('{zones_parquet.replace(os.sep, '/')}');")
    
    # Verify DuckDB table counts
    v_cnt = con.execute("SELECT count(*) FROM raw.vessels;").fetchone()[0]
    p_cnt = con.execute("SELECT count(*) FROM raw.ais_pings;").fetchone()[0]
    z_cnt = con.execute("SELECT count(*) FROM raw.high_risk_zones;").fetchone()[0]
    
    print(f"[DarkFleet-IQ DataGen] DuckDB Initialization Verified:")
    print(f"   raw.vessels:         {v_cnt:,} rows")
    print(f"   raw.ais_pings:       {p_cnt:,} rows")
    print(f"   raw.high_risk_zones: {z_cnt:,} rows")
    
    con.close()
    print("[DarkFleet-IQ DataGen] Pipeline completed successfully!")

if __name__ == "__main__":
    zones_df = pd.DataFrame(HIGH_RISK_ZONES)
    vessels_df = generate_vessel_registry(num_dark_vessels=35, num_normal_vessels=85)
    pings_df = generate_telemetry(vessels_df, sim_days=30)
    export_lakehouse(vessels_df, pings_df, zones_df)
