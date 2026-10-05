WITH vessels AS (
    SELECT * FROM {{ ref('stg_vessels') }}
),

pings_summary AS (
    SELECT
        mmsi,
        COUNT(*) AS total_ais_pings,
        MIN(ping_timestamp) AS first_seen_timestamp,
        MAX(ping_timestamp) AS last_seen_timestamp
    FROM {{ ref('stg_ais_pings') }}
    GROUP BY mmsi
),

dark_events_summary AS (
    SELECT
        mmsi,
        COUNT(*) AS total_dark_events,
        SUM(gap_hours) AS cumulative_dark_hours,
        MAX(gap_hours) AS max_single_gap_hours,
        MAX(abs_draft_delta_meters) AS max_draft_delta_m,
        SUM(CASE WHEN event_typology IN ('STS_CARGO_DISCHARGE_OFFLOAD', 'STS_CARGO_LOAD_TRANSFER') THEN 1 ELSE 0 END) AS total_sts_transfers,
        SUM(CASE WHEN zone_risk_level IN ('CRITICAL', 'HIGH') THEN 1 ELSE 0 END) AS high_risk_zone_events,
        SUM(estimated_barrels_transferred) AS total_illicit_barrels_est,
        SUM(estimated_cargo_value_usd) AS total_illicit_cargo_value_usd,
        MAX(forensic_severity_score) AS peak_forensic_severity_score
    FROM {{ ref('fct_dark_events') }}
    GROUP BY mmsi
),

combined AS (
    SELECT
        v.vessel_id,
        v.imo_number,
        v.mmsi,
        v.vessel_name,
        v.vessel_type,
        v.vessel_class,
        v.flag_country,
        v.flag_risk_category,
        v.flag_risk_score,
        v.deadweight_tonnage,
        v.max_design_draft_m,
        v.ballast_draft_m,
        v.build_year,
        v.vessel_age_years,
        v.is_dark_fleet_suspect AS ground_truth_dark_fleet_label,
        
        COALESCE(p.total_ais_pings, 0) AS total_ais_pings,
        p.first_seen_timestamp,
        p.last_seen_timestamp,
        
        COALESCE(d.total_dark_events, 0) AS total_dark_events,
        COALESCE(d.cumulative_dark_hours, 0.0) AS cumulative_dark_hours,
        COALESCE(d.max_single_gap_hours, 0.0) AS max_single_gap_hours,
        COALESCE(d.max_draft_delta_m, 0.0) AS max_draft_delta_m,
        COALESCE(d.total_sts_transfers, 0) AS total_sts_transfers,
        COALESCE(d.high_risk_zone_events, 0) AS high_risk_zone_events,
        COALESCE(d.total_illicit_barrels_est, 0.0) AS total_illicit_barrels_est,
        COALESCE(d.total_illicit_cargo_value_usd, 0.0) AS total_illicit_cargo_value_usd,
        COALESCE(d.peak_forensic_severity_score, 0.0) AS peak_forensic_severity_score
    FROM vessels v
    LEFT JOIN pings_summary p ON v.mmsi = p.mmsi
    LEFT JOIN dark_events_summary d ON v.mmsi = d.mmsi
),

scored AS (
    SELECT
        c.*,
        -- Composite Dark Fleet Risk Index (0 - 100)
        ROUND(
            LEAST(100.0,
                (c.total_sts_transfers * 35.0) +
                LEAST(25.0, c.cumulative_dark_hours * 0.4) +
                (c.high_risk_zone_events * 10.0) +
                (c.flag_risk_score * 7.0) +
                (CASE WHEN c.vessel_age_years >= 15 THEN 5.0 ELSE 0.0 END)
            ), 1
        ) AS dark_fleet_risk_index
    FROM combined c
)

SELECT
    s.*,
    -- Sanctions Risk Tier Classification
    CASE
        WHEN s.dark_fleet_risk_index >= 70.0 THEN 'CRITICAL_SANCTION_RISK'
        WHEN s.dark_fleet_risk_index >= 45.0 THEN 'HIGH_SUSPICION'
        WHEN s.dark_fleet_risk_index >= 20.0 THEN 'MODERATE_WATCHLIST'
        ELSE 'LOW_COMPLIANCE_RISK'
    END AS risk_tier,
    CASE
        WHEN s.dark_fleet_risk_index >= 70.0 THEN 'Mandate immediate voyage charter halt; OFAC / EU compliance escalation.'
        WHEN s.dark_fleet_risk_index >= 45.0 THEN 'Enhanced Due Diligence (EDD) required before bunkering or financing.'
        WHEN s.dark_fleet_risk_index >= 20.0 THEN 'Standard monitoring; audit AIS transponder maintenance logs.'
        ELSE 'Routine compliance clearance.'
    END AS recommended_regulatory_action
FROM scored s
