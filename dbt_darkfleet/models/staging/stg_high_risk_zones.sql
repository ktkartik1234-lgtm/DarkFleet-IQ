WITH raw_zones AS (
    SELECT * FROM raw.high_risk_zones
)

SELECT
    zone_id,
    TRIM(zone_name) AS zone_name,
    TRIM(sanctions_regime) AS sanctions_regime,
    TRIM(primary_threat) AS primary_threat,
    risk_level,
    CAST(min_lat AS DOUBLE) AS min_lat,
    CAST(max_lat AS DOUBLE) AS max_lat,
    CAST(min_lon AS DOUBLE) AS min_lon,
    CAST(max_lon AS DOUBLE) AS max_lon,
    CAST(center_lat AS DOUBLE) AS center_lat,
    CAST(center_lon AS DOUBLE) AS center_lon
FROM raw_zones
