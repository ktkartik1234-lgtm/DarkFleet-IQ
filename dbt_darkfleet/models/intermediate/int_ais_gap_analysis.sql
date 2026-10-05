WITH pings_windowed AS (
    SELECT
        ping_id,
        mmsi,
        ping_timestamp,
        latitude,
        longitude,
        speed_knots,
        course_over_ground,
        heading,
        draft_depth_meters,
        navigational_status,
        -- Window function LAG calculations partitioned by vessel MMSI (backward temporal window)
        LAG(ping_id)            OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS prev_ping_id,
        LAG(ping_timestamp)     OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS prev_ping_timestamp,
        LAG(latitude)           OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS prev_latitude,
        LAG(longitude)          OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS prev_longitude,
        LAG(draft_depth_meters) OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS prev_draft_depth_meters,
        LAG(speed_knots)        OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS prev_speed_knots,
        -- Window function LEAD calculations partitioned by vessel MMSI (forward temporal window)
        LEAD(ping_id)            OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS next_ping_id,
        LEAD(ping_timestamp)     OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS next_ping_timestamp,
        LEAD(latitude)           OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS next_latitude,
        LEAD(longitude)          OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS next_longitude,
        LEAD(draft_depth_meters) OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS next_draft_depth_meters,
        LEAD(speed_knots)        OVER (PARTITION BY mmsi ORDER BY ping_timestamp) AS next_speed_knots
    FROM {{ ref('stg_ais_pings') }}
),

metrics_calculated AS (
    SELECT
        p.*,
        -- Backward temporal & spatial deltas (LAG-based)
        ROUND(date_diff('second', p.prev_ping_timestamp, p.ping_timestamp) / 3600.0, 2) AS gap_hours,
        ROUND(p.draft_depth_meters - p.prev_draft_depth_meters, 2) AS draft_delta_meters,
        ROUND(ABS(p.draft_depth_meters - p.prev_draft_depth_meters), 2) AS abs_draft_delta_meters,
        -- Haversine drift distance (kilometers) from previous ping
        ROUND(
            2.0 * 6371.0 * ASIN(
                SQRT(
                    LEAST(1.0, GREATEST(0.0,
                        POWER(SIN(RADIANS(p.latitude - p.prev_latitude) / 2.0), 2) +
                        COS(RADIANS(p.prev_latitude)) * COS(RADIANS(p.latitude)) *
                        POWER(SIN(RADIANS(p.longitude - p.prev_longitude) / 2.0), 2)
                    ))
                )
            ), 2
        ) AS drift_distance_km,
        -- Forward temporal & spatial deltas (LEAD-based)
        ROUND(date_diff('second', p.ping_timestamp, p.next_ping_timestamp) / 3600.0, 2) AS next_gap_hours,
        ROUND(p.next_draft_depth_meters - p.draft_depth_meters, 2) AS next_draft_delta_meters,
        ROUND(
            2.0 * 6371.0 * ASIN(
                SQRT(
                    LEAST(1.0, GREATEST(0.0,
                        POWER(SIN(RADIANS(p.next_latitude - p.latitude) / 2.0), 2) +
                        COS(RADIANS(p.latitude)) * COS(RADIANS(p.next_latitude)) *
                        POWER(SIN(RADIANS(p.next_longitude - p.longitude) / 2.0), 2)
                    ))
                )
            ), 2
        ) AS next_drift_distance_km
    FROM pings_windowed p
    WHERE p.prev_ping_timestamp IS NOT NULL
),

zone_matches AS (
    SELECT
        m.ping_id,
        z.zone_id,
        z.zone_name,
        z.sanctions_regime,
        z.risk_level,
        ROW_NUMBER() OVER (
            PARTITION BY m.ping_id 
            ORDER BY 
                CASE WHEN z.risk_level = 'CRITICAL' THEN 1 WHEN z.risk_level = 'HIGH' THEN 2 ELSE 3 END,
                SQRT(POWER(m.latitude - z.center_lat, 2) + POWER(m.longitude - z.center_lon, 2)) ASC
        ) AS match_priority
    FROM metrics_calculated m
    INNER JOIN {{ ref('stg_high_risk_zones') }} z
        ON (
            (m.prev_latitude BETWEEN z.min_lat AND z.max_lat AND m.prev_longitude BETWEEN z.min_lon AND z.max_lon)
            OR
            (m.latitude BETWEEN z.min_lat AND z.max_lat AND m.longitude BETWEEN z.min_lon AND z.max_lon)
        )
),

with_zones AS (
    SELECT
        m.*,
        ROUND(m.drift_distance_km / 1.852, 2) AS drift_distance_nm,
        ROUND(
            m.drift_distance_km / NULLIF(m.gap_hours, 0.0), 2
        ) AS implied_speed_kmh,
        ROUND(
            (m.drift_distance_km / 1.852) / NULLIF(m.gap_hours, 0.0), 2
        ) AS implied_speed_knots,
        -- Spatial proximity mapping to high-risk geopolitical zones (deduplicated)
        z.zone_id,
        COALESCE(z.zone_name, 'International Waters / Open Sea') AS zone_name,
        COALESCE(z.sanctions_regime, 'Unregulated Commercial Corridor') AS sanctions_regime,
        COALESCE(z.risk_level, 'STANDARD') AS zone_risk_level,
        CASE
            WHEN m.gap_hours >= 12.0 THEN TRUE
            ELSE FALSE
        END AS is_transponder_gap,
        CASE
            WHEN m.gap_hours >= 12.0 AND m.abs_draft_delta_meters >= 1.5 THEN TRUE
            ELSE FALSE
        END AS is_suspect_sts_gap,
        -- Forward blackout entry indicator (LEAD-based)
        CASE
            WHEN m.next_gap_hours >= 12.0 THEN TRUE
            ELSE FALSE
        END AS is_entering_blackout
    FROM metrics_calculated m
    LEFT JOIN zone_matches z
        ON m.ping_id = z.ping_id AND z.match_priority = 1
)

SELECT * FROM with_zones
