UPDATE `best-start-main.ga4_transformed.ga4_goal_channel` AS ga4
SET ga4.sessionManualAdContent = src.old_creative
FROM (
  WITH dash AS (
    SELECT DISTINCT 
      TRIM(creative_descr) AS old_creative,
      --campaign_name AS platform_campaigns,
      REGEXP_REPLACE(TRIM(creative_descr), r'[_\-\s]+', '') AS creative_name,
      ROW_NUMBER() OVER(PARTITION BY LOWER(creative_descr)) AS row_num
    FROM `best-startn-main.dash_table.dash_union`
  ),
  deduplicate_dash AS (
    SELECT old_creative,creative_name FROM dash WHERE row_num=1
  ),
  ga4_tmp AS (
    SELECT DISTINCT TRIM(sessionManualAdContent) as ga4_creative_name,
      --campaign_name AS ga4_campaigns,
      REGEXP_REPLACE(TRIM(sessionManualAdContent), r'[_\-\s]+', '') AS creative_name,
      ROW_NUMBER() OVER (PARTITION BY LOWER(sessionManualAdContent)) AS row_num
    FROM `best-start-main.ga4_transformed.ga4_goal_channel`
  ),
  deduplicate_ga4 AS (
    SELECT ga4_creative_name,creative_name
    FROM ga4_tmp
    WHERE row_num=1
  ),
  result AS (
  SELECT DISTINCT 
    ga4.ga4_creative_name,
    --ga4_tmp.ga4_campaigns,
    dash.old_creative,
    ROW_NUMBER() OVER (PARTITION BY LOWER(ga4.ga4_creative_name)) AS row_num
    --dash.date
  FROM deduplicate_ga4 AS ga4
  LEFT JOIN deduplicate_dash AS dash
    ON LOWER(dash.creative_name) = LOWER(ga4.creative_name)
  WHERE dash.creative_name IS NOT NULL)
  SELECT *EXCEPT(row_num) FROM result WHERE row_num=1
) AS src
WHERE lower(trim(ga4.sessionManualAdContent)) = lower(trim(src.ga4_creative_name))