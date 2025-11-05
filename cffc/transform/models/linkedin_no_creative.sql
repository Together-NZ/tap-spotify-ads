  {{ config(
    materialized='table',
) }}

WITH daily_stats AS (SELECT
  SAFE_CAST(JSON_VALUE(data, '$.video_completions') AS INT64) AS video_completion,
  SAFE_CAST(JSON_VALUE(data, '$.comments') AS INT64) AS comments,
  JSON_VALUE(data, '$.cost_in_usd') AS cost_in_usd,
  SAFE_CAST(JSON_VALUE(data, '$.cost_in_local_currency') AS FLOAT64) AS media_cost,
  SAFE_CAST(JSON_VALUE(data, '$.impressions') AS INT64) AS impressions,
  SAFE_CAST(JSON_VALUE(data, '$.clicks') AS INT64) AS clicks,
  SAFE_CAST(JSON_VALUE(data, '$.total_engagements') AS INT64) AS totalEngagements,
  SAFE_CAST(JSON_VALUE(data, '$.full_screen_plays') AS INT64) AS fullScreenPlays,
  SAFE_CAST(JSON_VALUE(data, '$.video_starts') AS INT64) AS video_starts,
  SAFE_CAST(JSON_VALUE(data, '$.video_views') AS INT64) AS video_views,
  SAFE_CAST(JSON_VALUE(data, '$.video_first_quartile_completions') AS INT64) AS video_25_completion,
  SAFE_CAST(JSON_VALUE(data, '$.video_midpoint_completions') AS INT64) AS video_50_completion,
  SAFE_CAST(JSON_VALUE(data, '$.video_third_quartile_completions') AS INT64) AS video_75_completion,
  SAFE_CAST(JSON_VALUE(data, '$.likes') AS INT64) AS likes,
  SAFE_CAST(JSON_VALUE(data, '$.follows') AS INT64) AS follows,
  SAFE_CAST(JSON_VALUE(data, '$.comment_likes') AS INT64) AS commentLikes,
  SAFE_CAST(JSON_VALUE(data, '$.landing_page_clicks') AS INT64) AS landingPageClicks,
  DATE(
  TIMESTAMP(JSON_VALUE(data, '$.start_at')),
  "Pacific/Auckland"
  ) AS date,


  JSON_VALUE(data, '$.creative_id') AS creative_id,
FROM
  `cffc-main.linkedin_raw.ad_analytics_by_creative`),
creative_campaign_link AS (
  SELECT
  SPLIT(JSON_VALUE(data,'$.campaign'),':')[3] AS campaign_id,
  SPLIT(JSON_VALUE(data,'$.id'),':')[3] AS creative_id_1
  FROM `cffc-main.linkedin_raw.creatives`
),final AS (
SELECT *, ROW_NUMBER() OVER (PARTITION BY daily_stats.creative_id,date,creative_id_1) AS row_num FROM daily_stats LEFT JOIN creative_campaign_link ON
daily_stats.creative_id = creative_campaign_link.creative_id_1 ),
stats_by_creative_campaign_id AS (
SELECT * except(row_num) FROM final where row_num = 1),
campaign_stats AS (      SELECT
    JSON_VALUE(data, '$.id') AS campaign_id,
    JSON_VALUE(data, '$.account_id') as advertiser_account_id,
    JSON_VALUE(data, '$.name') AS campaign_name,
    JSON_VALUE(data, '$.objective_type') as campaign_type,
    JSON_VALUE(data, '$.totalBudget.amount') as campaign_budget,
    JSON_VALUE(data, '$.status') as campaign_status,
    FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.start') as int64)), "Pacific/Auckland")) AS campaign_start_date,
    FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.end') as int64)), "Pacific/Auckland")) AS camapign_end_date,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data, '$.id'),JSON_VALUE(data, '$.account_id'),FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.start') as int64)), "Pacific/Auckland")), FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.end') as int64)), "Pacific/Auckland")) ) as row_num
    from `cffc-main.linkedin_raw.campaigns`),
deduplicated_campaign_data AS (SELECT * EXCEPT(row_num) FROM campaign_stats where row_num = 1)
SELECT stat_creative.*, campaign_data.* EXCEPT(campaign_id) FROM 
stats_by_creative_campaign_id AS stat_creative LEFT JOIN  deduplicated_campaign_data as campaign_data
ON stat_creative.campaign_id = campaign_data.campaign_id 