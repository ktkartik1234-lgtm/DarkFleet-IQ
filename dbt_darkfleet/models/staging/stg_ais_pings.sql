WITH raw_pings AS (
    SELECT * FROM raw.ais_pings
)

SELECT
    CAST(ping_id AS BIGINT) AS ping_id,
    CAST(mmsi AS BIGINT) AS mmsi,
    CAST(timestamp AS TIMESTAMP) AS ping_timestamp,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(speed_knots AS DOUBLE) AS speed_knots,
    CAST(course_over_ground AS DOUBLE) AS course_over_ground,
    CAST(heading AS INTEGER) AS heading,
    CAST(draft_depth_meters AS DOUBLE) AS draft_depth_meters,
    TRIM(navigational_status) AS navigational_status
FROM raw_pings
WHERE latitude BETWEEN -90.0 AND 90.0
  AND longitude BETWEEN -180.0 AND 180.0
  AND draft_depth_meters >= 0.0
