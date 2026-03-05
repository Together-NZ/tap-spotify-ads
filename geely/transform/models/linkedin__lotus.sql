  {{ config(
    materialized='table',
) }}

WITH daily_stats_raw AS (SELECT
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
  _sdc_extracted_at AS updated_at,

  JSON_VALUE(data, '$.creative_id') AS creative_id,
  ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.creative_id'),  JSON_VALUE(data, '$.start_at') ORDER BY _sdc_extracted_at DESC) AS row_num
FROM
  `geely-main.linkedin_raw__lotus.ad_analytics_by_creative`),
  daily_stats AS (
    SELECT * EXCEPT(row_num)  FROM daily_stats_raw WHERE row_num = 1
  ),
creative_campaign_link_raw AS (
  SELECT
  SPLIT(JSON_VALUE(data,'$.campaign'),':')[3] AS campaign_id,
  SPLIT(JSON_VALUE(data,'$.id'),':')[3] AS creative_id_1,
  JSON_VALUE(data,'$.name') AS creative_name,
  ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.last_modified_time'),_sdc_extracted_at DESC) AS row_num
  
  FROM `geely-main.linkedin_raw__lotus.creatives`
),
creative_campaign_link AS (
  SELECT * EXCEPT(row_num) FROM creative_campaign_link_raw WHERE row_num = 1
),
campaign_group_campaign_link AS (
  SELECT DISTINCT JSON_VALUE(data,'$.name') AS campaign_group_name,JSON_VALUE(data,'$.id') AS campaign_group_id,date(timestamp(JSON_VALUE(data,'$.last_modified_time')) ,'Pacific/Auckland')as modified_date, ROW_NUMBER() OVER(PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.last_modified_time') DESC) AS row_num
   FROM `geely-main.linkedin_raw__lotus.campaign_groups` 
  ),
distinct_campaign_group_campaign_link AS (
  SELECT * FROM campaign_group_campaign_link WHERE modified_date > '2025-09-01' and row_num=1 and lower(campaign_group_name) like '%uow%'
),


final AS (
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
    JSON_VALUE(data,'$.campaign_group_id') AS campaign_group_id,
    FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.start') as int64)), "Pacific/Auckland")) AS campaign_start_date,
    FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.end') as int64)), "Pacific/Auckland")) AS camapign_end_date,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data, '$.id'),JSON_VALUE(data, '$.account_id'),FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.start') as int64)), "Pacific/Auckland")), FORMAT_DATE('%F', DATETIME(TIMESTAMP_MILLIS(safe_cast(JSON_VALUE(data, '$.runSchedule.end') as int64)), "Pacific/Auckland")) ) as row_num
    from `geely-main.linkedin_raw__lotus.campaigns`),
deduplicated_campaign_data AS (SELECT * EXCEPT(row_num) FROM campaign_stats where row_num = 1),
joint_campaign_group as (
  SELECT deduplicated_campaign_data.*,distinct_campaign_group_campaign_link.campaign_group_name
  FROM deduplicated_campaign_data LEFT JOIN distinct_campaign_group_campaign_link ON deduplicated_campaign_data.campaign_group_id = distinct_campaign_group_campaign_link.campaign_group_id

),
update_campaign_name as (
  select joint_campaign_group.* EXCEPT(campaign_name,campaign_group_name),
  CASE WHEN campaign_group_name is null then campaign_name else campaign_group_name 
  END AS campaign_name 
  FROM joint_campaign_group
)
SELECT stat_creative.*, campaign_data.* EXCEPT(campaign_id) FROM 
stats_by_creative_campaign_id AS stat_creative LEFT JOIN  update_campaign_name as campaign_data
ON stat_creative.campaign_id = campaign_data.campaign_id 