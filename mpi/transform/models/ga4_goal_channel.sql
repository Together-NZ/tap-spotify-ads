{{ config(
    materialized='table',
) }}
WITH 
table1 AS (SELECT
  -- keep your dimensions here
  date,
  eventName,
  CASE WHEN eventName = 'averageSessionDuration' 
    THEN 
      NULL 
      ELSE metric_value
  END AS eventCount,
  CASE WHEN eventName = 'averageSessionDuration' 
    THEN 
      metric_value
    ELSE NULL 
  END AS eventValue,
  campaign_name, publisher, sessionSourceMedium, sessionCampaignName, sessionSourceMediumraw, site_name, channel, campaign_name_selection, sessionManualAdContent, funnel, media_format 
     

FROM `mpi-main.ga4_transformed.ga4_goal_channel_session`
UNPIVOT (
  metric_value FOR eventName IN (
    sessions,
    averageSessionDuration,
    engagedSession,
    userEngagementDuration
  )
)),
table2 AS (
    SELECT * FROM `mpi-main.ga4_transformed.ga4_goal_channel_goal`
),
union_table AS (
SELECT date,eventName,safe_cast(eventCount AS INT64) as eventCount,safe_cast(eventValue AS FLOAT64) as eventValue, campaign_name, publisher, sessionSourceMedium, sessionCampaignName, sessionSourceMediumraw, site_name, channel, campaign_name_selection, sessionManualAdContent, funnel, media_format FROM table1
UNION ALL
SELECT  date,eventName,eventCount ,SAFE_CAST(eventValue AS FLOAT64) as eventValue , campaign_name, publisher, sessionSourceMedium, sessionCampaignName, sessionSourceMediumraw, site_name, channel, campaign_name_selection, sessionManualAdContent, funnel, media_format FROM table2
),
session_sessionDuration_reference AS (
  SELECT *
  FROM union_table
  WHERE eventName IN ('sessions', 'averageSessionDuration')
   
),
sessions_ref AS (
  SELECT
    date,
    publisher,
    campaign_name,
    sessionManualAdContent,
    sessionSourceMedium,
    eventCount AS sessions_count
  FROM session_sessionDuration_reference
  WHERE eventName = 'sessions'
),
joint AS (
  SELECT
  r.date,
  r.eventName,
  -- for avg duration: use sessions' eventCount from the matching session row
  CASE
    WHEN r.eventName = 'averageSessionDuration' THEN s.sessions_count
    ELSE r.eventCount
  END AS eventCount,
  -- keep value only for averageSessionDuration (optional: you can also keep it for sessions if you want)
  CASE
    WHEN r.eventName = 'sessions' THEN NULL
    ELSE r.eventValue
  END AS eventValue,
  r.campaign_name, 
  r.publisher, 
  r.sessionSourceMedium, 
  r.sessionCampaignName, 
  r.sessionSourceMediumraw,
  r.site_name, 
  r.channel, 
  r.campaign_name_selection, 
  r.sessionManualAdContent, r.funnel, r.media_format
FROM session_sessionDuration_reference r
LEFT JOIN sessions_ref s
  ON  r.date = s.date
  AND r.publisher = s.publisher
  AND r.campaign_name = s.campaign_name
  AND r.sessionManualAdContent = s.sessionManualAdContent
  AND r.sessionSourceMedium = s.sessionSourceMedium
),
deduplicated_joint AS (
  select * from joint 
),
final AS (
SELECT date,eventName,safe_cast(eventCount AS INT64) as eventCount,safe_cast(eventValue AS FLOAT64) as eventValue, campaign_name, publisher, sessionSourceMedium, sessionCampaignName, sessionSourceMediumraw, site_name, channel, campaign_name_selection, sessionManualAdContent, funnel, media_format FROM table2
UNION ALL
SELECT  date,eventName,eventCount ,SAFE_CAST(eventValue AS FLOAT64) as eventValue , campaign_name, publisher, sessionSourceMedium, sessionCampaignName, sessionSourceMediumraw, site_name, channel, campaign_name_selection, sessionManualAdContent, funnel, media_format FROM deduplicated_joint)
,deduplicated_final as (
  select *,
  ROW_NUMBER() OVER (PARTITION BY date,publisher,eventCount,SAFE_CAST(eventValue as STRING),sessionManualAdContent,
sessionCampaignName,sessionSourceMedium,eventName) as row_num from final
)
select * from deduplicated_final where row_num=1