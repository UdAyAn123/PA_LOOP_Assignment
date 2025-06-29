WITH
LatestGrubhub AS (
  SELECT
    slug AS gh_slug,
    b_name,
    vb_name,
    response,
    timestamp
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY slug ORDER BY timestamp DESC) AS rn
    FROM `arboreal-vision-339901.take_home_v2.virtual_kitchen_grubhub_hours`
  )
  WHERE rn = 1
),


LatestUberEats AS (
  SELECT
    slug AS ue_slug,
    b_name,
    vb_name,
    response,
    timestamp
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY slug ORDER BY timestamp DESC) AS rn
    FROM `arboreal-vision-339901.take_home_v2.virtual_kitchen_ubereats_hours`
  )
  WHERE rn = 1
),


Grubhub AS (
  SELECT
    gh_slug,
    b_name,
    vb_name,
    JSON_VALUE(response, '$.today_availability_by_catalog.STANDARD_DELIVERY[0].from') AS gh_start,
    JSON_VALUE(response, '$.today_availability_by_catalog.STANDARD_DELIVERY[0].to') AS gh_end
  FROM LatestGrubhub
),


UberEats AS (
  SELECT
    ue_slug,
    b_name,
    vb_name,
    JSON_VALUE(response, '$.data.menus."26bd579e-5664-4f0a-8465-2f5eb5fbe705".sections[0].regularHours[0].startTime') AS ue_start,
    JSON_VALUE(response, '$.data.menus."26bd579e-5664-4f0a-8465-2f5eb5fbe705".sections[0].regularHours[0].endTime') AS ue_end
  FROM LatestUberEats
),


TimeComparison AS (
  SELECT
    gh.gh_slug,
    CONCAT(gh.gh_start, ' - ', gh.gh_end) AS gh_business_hours,
    ue.ue_slug,
    CONCAT(ue.ue_start, ' - ', ue.ue_end) AS ue_business_hours,
    CASE
      WHEN PARSE_TIME('%H:%M:%S', SUBSTR(gh.gh_start, 1, 8)) >= PARSE_TIME('%H:%M', ue.ue_start)
        AND PARSE_TIME('%H:%M:%S', SUBSTR(gh.gh_end, 1, 8)) <= PARSE_TIME('%H:%M', ue.ue_end)
        THEN 'In Range'
      WHEN ABS(TIME_DIFF(
             PARSE_TIME('%H:%M:%S', SUBSTR(gh.gh_start, 1, 8)),
             PARSE_TIME('%H:%M', ue.ue_start),
             MINUTE)) <= 5
        AND ABS(TIME_DIFF(
             PARSE_TIME('%H:%M:%S', SUBSTR(gh.gh_end, 1, 8)),
             PARSE_TIME('%H:%M', ue.ue_end),
             MINUTE)) <= 5
        THEN 'Out of Range with 5 mins difference'
      ELSE 'Out of Range'
    END AS is_out_of_range
  FROM Grubhub gh
  INNER JOIN UberEats ue
    ON gh.b_name = ue.b_name
    AND gh.vb_name = ue.vb_name
)


SELECT * FROM TimeComparison
