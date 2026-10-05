WITH zones AS (
    SELECT * FROM {{ ref('stg_high_risk_zones') }}
)

SELECT
    zone_id,
    zone_name,
    sanctions_regime,
    primary_threat,
    risk_level,
    min_lat,
    max_lat,
    min_lon,
    max_lon,
    center_lat,
    center_lon,
    ROUND((max_lat - min_lat) * 111.0, 1) AS lat_span_km,
    ROUND((max_lon - min_lon) * 111.0 * COS(RADIANS(center_lat)), 1) AS lon_span_km,
    CASE 
        WHEN risk_level = 'CRITICAL' THEN 'Immediate OFAC / Maritime Sanctions Scrutiny'
        WHEN risk_level = 'HIGH' THEN 'Price-Cap & G7 Enforcement Priority'
        ELSE 'Monitored Trade Corridor'
    END AS compliance_directive
FROM zones
