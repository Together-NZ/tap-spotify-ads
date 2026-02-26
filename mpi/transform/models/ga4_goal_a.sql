{{ config(
    materialized='table',
) }}

WITH deduplicated_data AS (
  SELECT 
    PARSE_DATE('%Y%m%d', JSON_VALUE(data, '$.date')) AS date,
    IF (CONTAINS_SUBSTR(JSON_VALUE(data, '$.sessionSourceMedium'), '/'),
        INITCAP(SPLIT(JSON_VALUE(data, '$.sessionSourceMedium'), '/')[SAFE_OFFSET(0)]),
        INITCAP(JSON_VALUE(data, '$.sessionSourceMedium'))) AS sessionSourceMedium,
    JSON_VALUE(data, '$.sessionCampaignName') AS sessionCampaignName,
    JSON_VALUE(data, '$.sessionCampaignName') AS campaign_name,
    JSON_VALUE(data, '$.sessionManualAdContent') AS sessionManualAdContent,
    JSON_VALUE(data, '$.eventName') AS eventName,
    SAFE_CAST(JSON_VALUE(data, '$.eventCount') AS INT64) AS eventCount,
    JSON_VALUE(data, '$.eventValue') AS eventValue,
    JSON_VALUE(data, '$.report_start_date') AS report_start_date,
    JSON_VALUE(data, '$.report_end_date') AS report_end_date,
    JSON_VALUE(data,'$.sessionSourceMedium') AS sessionSourceMediumraw,
    json_value(data,'$.hostName') AS hostName,
    json_value(data,'$.sessions') AS sessions,
    JSON_VALUE(data,'$.engagedSession') AS engagedSession,
    JSON_VALUE(data, '$.userEngagementDuration') AS userEngagementDuration,
    JSON_VALUE(data, '$.averageSessionDuration') AS averageSessionDuration,
    JSON_VALUE(data,'$."customEvent:serviceType[screen_view]"') AS serviceType,
    _sdc_extracted_at,
    _sdc_received_at,
    _sdc_batched_at,
    _sdc_deleted_at,
    _sdc_sequence,
    _sdc_table_version,
    -- Apply the CASE statement here to transform raw_sessionSourceMedium into categories
    CASE 
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%organic%' THEN 'organic_search'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%direct%' 
              AND JSON_VALUE(data, '$.sessionCampaignName') NOT LIKE 'wat-' THEN 'direct'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%email%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%mailout%' 
              OR (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%automated%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%email%') THEN 'email'
        WHEN (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%'
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%meta%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%social%')
              AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'meta'
        WHEN JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%search%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%sem%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%performance max%'
                  or lower(json_value(data, '$.sessionCampaignName')) like '%pmax%')
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%google / cpc%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%_uow0%'
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%wat-%') THEN 'google_ads_search'
        WHEN (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%googleads%' 
              OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%googleads%' 
              OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%native%' 
              OR (LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%demand%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%gen%'))
            OR (JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
                  AND JSON_VALUE(data, '$.sessionCampaignName') LIKE 'wat-%')
            OR (JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
                  AND JSON_VALUE(data, '$.sessionCampaignName') LIKE '_uow%'
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%sem%'
              AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%search%') THEN 'demand_gen'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%kargo%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'kargo'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'linkedin'
        WHEN ((LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%')
              AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%cpm%' 
              AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%cpc%')
            OR ((LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%')
                  AND JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%referral%') THEN 'own_social'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%referral%' THEN 'referral'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%snapchat%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'snapchat'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%spotify%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'spotify'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%stuff%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'stuff'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%tiktok%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'tiktok'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'twitter'
        ELSE SPLIT(JSON_VALUE(data, '$.sessionSourceMedium'),'/')[OFFSET(0)]
    END AS site_name,
    
    ROW_NUMBER() OVER (
      PARTITION BY 
        PARSE_DATE('%Y%m%d', JSON_VALUE(data, '$.date')),
        JSON_VALUE(data, '$.sessionSourceMedium'),
        JSON_VALUE(data, '$.sessionCampaignName'),
        JSON_VALUE(data, '$.sessionManualAdContent'),
        
        JSON_VALUE(data, '$.eventName'),
        json_value(data,'$.eventCount'),
        JSON_VALUE(data,'$.eventValue')
      ORDER BY _sdc_extracted_at DESC
    ) AS row_num

  FROM 
    `mpi-main`.`ga4_raw`.`goal`

),

filtered_creatives as (
  SELECT * except(sessionManualAdContent),
  CASE WHEN LOWER(sessionManualAdContent) like '%mpi%' THEN SPLIT(sessionManualAdContent,'_')[OFFSET(ARRAY_LENGTH(SPLIT(sessionManualAdContent,'_'))-1)]
  else sessionManualAdContent
  end as sessionManualAdContent
  from deduplicated_data
),
final_result AS (
-- Select only the latest row for each unique combination of keys
SELECT
  date,
  sessionSourceMedium,
  hostName,
  sessions,
  engagedSession,
  userEngagementDuration,
  averageSessionDuration,
  serviceType,
  sessionCampaignName,
  campaign_name,
  sessionManualAdContent,
  sessionSourceMediumraw,
  eventName,
  eventCount,
  eventValue,
  report_start_date,
  report_end_date,
  _sdc_extracted_at,
  _sdc_received_at,
  _sdc_batched_at,
  _sdc_deleted_at,
  _sdc_sequence,
  _sdc_table_version,
  site_name
FROM filtered_creatives
WHERE row_num = 1 
)
SELECT * 
FROM final_result 