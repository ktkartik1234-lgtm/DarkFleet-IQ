WITH raw_vessels AS (
    SELECT * FROM raw.vessels
)

SELECT
    vessel_id,
    CAST(imo_number AS BIGINT) AS imo_number,
    CAST(mmsi AS BIGINT) AS mmsi,
    TRIM(vessel_name) AS vessel_name,
    vessel_type,
    vessel_class,
    TRIM(flag_country) AS flag_country,
    flag_risk_category,
    CAST(deadweight_tonnage AS INTEGER) AS deadweight_tonnage,
    CAST(max_design_draft_m AS DOUBLE) AS max_design_draft_m,
    CAST(ballast_draft_m AS DOUBLE) AS ballast_draft_m,
    CAST(build_year AS INTEGER) AS build_year,
    CAST(vessel_age_years AS INTEGER) AS vessel_age_years,
    CAST(is_dark_fleet_suspect AS BOOLEAN) AS is_dark_fleet_suspect,
    CASE 
        WHEN flag_risk_category = 'HIGH_RISK_SANCTION_FLAG' THEN 3
        WHEN flag_risk_category = 'FLAG_OF_CONVENIENCE' THEN 2
        ELSE 1
    END AS flag_risk_score
FROM raw_vessels
