WITH gaps AS (
    SELECT * FROM {{ ref('int_ais_gap_analysis') }}
    WHERE gap_hours >= 12.0
),

vessels AS (
    SELECT * FROM {{ ref('stg_vessels') }}
),

events_joined AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY g.ping_timestamp, g.ping_id) AS event_id,
        g.ping_id,
        g.prev_ping_id AS disconnect_ping_id,
        g.mmsi,
        v.vessel_id,
        v.imo_number,
        v.vessel_name,
        v.vessel_type,
        v.vessel_class,
        v.flag_country,
        v.flag_risk_category,
        v.flag_risk_score,
        v.deadweight_tonnage,
        v.max_design_draft_m,
        v.ballast_draft_m,
        g.prev_ping_timestamp AS blackout_start_timestamp,
        g.ping_timestamp AS blackout_end_timestamp,
        g.gap_hours,
        g.prev_latitude AS disconnect_latitude,
        g.prev_longitude AS disconnect_longitude,
        g.latitude AS reconnect_latitude,
        g.longitude AS reconnect_longitude,
        g.drift_distance_km,
        g.drift_distance_nm,
        g.implied_speed_knots,
        g.prev_draft_depth_meters AS disconnect_draft_m,
        g.draft_depth_meters AS reconnect_draft_m,
        g.draft_delta_meters,
        g.abs_draft_delta_meters,
        g.zone_id,
        g.zone_name,
        g.sanctions_regime,
        g.zone_risk_level,
        -- Forensic Typology Classification
        CASE
            WHEN g.draft_delta_meters <= -1.5 THEN 'STS_CARGO_DISCHARGE_OFFLOAD'
            WHEN g.draft_delta_meters >= 1.5 THEN 'STS_CARGO_LOAD_TRANSFER'
            WHEN g.zone_risk_level IN ('CRITICAL', 'HIGH') THEN 'SPOOFING_OR_SUSPECT_CHOKEPOINT_BLACKOUT'
            ELSE 'ROUTINE_TRANSPONDER_LOSS'
        END AS event_typology,
        
        -- Physical Estimation of Transferred Cargo (Bbls & USD Value @ $75/bbl)
        CASE
            WHEN g.abs_draft_delta_meters >= 1.5 AND v.vessel_type IN ('Crude Oil Tanker', 'Product Tanker') THEN
                ROUND(
                    (v.deadweight_tonnage * (g.abs_draft_delta_meters / NULLIF(v.max_design_draft_m - v.ballast_draft_m, 0.0))) * 7.33,
                    0
                )
            ELSE 0.0
        END AS estimated_barrels_transferred
    FROM gaps g
    INNER JOIN vessels v ON g.mmsi = v.mmsi
),

scored_events AS (
    SELECT
        e.*,
        ROUND(e.estimated_barrels_transferred * 75.0, 2) AS estimated_cargo_value_usd,
        -- Severity Scoring Algorithm (0-100)
        ROUND(
            LEAST(100.0,
                -- Blackout duration score (up to 35 pts)
                LEAST(35.0, e.gap_hours * 0.75) +
                -- Draft delta score (up to 40 pts)
                LEAST(40.0, e.abs_draft_delta_meters * 5.5) +
                -- High risk zone score (up to 15 pts)
                CASE 
                    WHEN e.zone_risk_level = 'CRITICAL' THEN 15.0
                    WHEN e.zone_risk_level = 'HIGH' THEN 10.0
                    ELSE 0.0
                END +
                -- Flag compliance risk score (up to 10 pts)
                CASE 
                    WHEN e.flag_risk_category = 'HIGH_RISK_SANCTION_FLAG' THEN 10.0
                    WHEN e.flag_risk_category = 'FLAG_OF_CONVENIENCE' THEN 5.0
                    ELSE 0.0
                END
            ), 1
        ) AS forensic_severity_score
    FROM events_joined e
)

SELECT * FROM scored_events
