  {{ config(
    materialized='table',
) }} 
WITH 
clean_ads AS (
    SELECT * EXCEPT (row_number) FROM (
         SELECT JSON_VALUE(data,'$.id') AS ad_id,
    JSON_VALUE(data,'$.name') AS ad_name,
    JSON_VALUE(data,'$.ad_set_id') AS ad_set_id,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.updated_at') DESC) AS row_number
    FROM `together-internal.spotify_raw.ads` 
    ) WHERE row_number = 1
),
clean_ad_sets AS (
    SELECT * EXCEPT (row_number) FROM (
        SELECT JSON_VALUE(data,'$.id') AS adset_id,
        JSON_VALUE(data,'$.name') AS adset_name,
        JSON_VALUE(data,'$.campaign_id') AS campaign_id,
        ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.updated_at') DESC) AS row_number
        FROM `together-internal.spotify_raw.ad_sets`
    ) WHERE row_number = 1
),
clean_campaigns AS (
    SELECT * EXCEPT (row_number) FROM (
        SELECT JSON_VALUE(data,'$.id') AS campaign_id,
        JSON_VALUE(data,'$.name') AS campaign_name,
        JSON_VALUE(data,'$.objective') AS objective,
        ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.updated_at') DESC) AS row_number
        FROM `together-internal.spotify_raw.campaigns`
    ) WHERE row_number = 1
),
ad_adset_campaign AS (
SELECT ad_id,ad_name, adsets.adset_id,adsets.adset_name,adsets.campaign_id,campaigns.campaign_name
FROM clean_ads AS ads LEFT JOIN 
clean_ad_sets AS adsets ON ads.ad_set_id = adsets.adset_id LEFT JOIN
clean_campaigns AS campaigns ON adsets.campaign_id = campaigns.campaign_id
),
clean_stats AS (
    SELECT * EXCEPT (row_number) FROM (
        SELECT
            JSON_VALUE(data, '$.entity_id') AS ad_id,
            JSON_VALUE(data, '$.entity_name') AS ad_name,
            JSON_VALUE(data, '$.entity_status') AS ad_status,
            CAST(JSON_VALUE(data, '$.start_time') AS TIMESTAMP) AS start_time,
            CAST(JSON_VALUE(data, '$.end_time') AS TIMESTAMP) AS end_time,
            CAST(JSON_VALUE(data, '$.stats[0].field_value') AS FLOAT64) AS clicks,
            CAST(JSON_VALUE(data, '$.stats[1].field_value') AS FLOAT64) AS video_25_completion,
            CAST(JSON_VALUE(data, '$.stats[2].field_value') AS FLOAT64) AS impressions,
            CAST(JSON_VALUE(data, '$.stats[3].field_value') AS FLOAT64) AS video_50_completion,
            CAST(JSON_VALUE(data, '$.stats[4].field_value') AS FLOAT64) AS media_cost,
            CAST(JSON_VALUE(data, '$.stats[5].field_value') AS FLOAT64) AS reach,
            CAST(JSON_VALUE(data, '$.stats[6].field_value') AS FLOAT64) AS video_views,
            CAST(JSON_VALUE(data, '$.stats[7].field_value') AS FLOAT64) AS video_75_completion,
            CAST(JSON_VALUE(data, '$.stats[8].field_value') AS FLOAT64) AS leads,
            CAST(JSON_VALUE(data, '$.stats[9].field_value') AS FLOAT64) AS starts,
            CAST(JSON_VALUE(data, '$.stats[10].field_value') AS FLOAT64) AS purchases,
            CAST(JSON_VALUE(data, '$.stats[11].field_value') AS FLOAT64) AS revenue,
            CAST(JSON_VALUE(data, '$.stats[12].field_value') AS FLOAT64) AS page_views,
            CAST(JSON_VALUE(data, '$.stats[13].field_value') AS FLOAT64) AS add_to_cart,
            CAST(JSON_VALUE(data, '$.stats[14].field_value') AS FLOAT64) AS completes,
            CAST(JSON_VALUE(data, '$.stats[15].field_value') AS FLOAT64) AS sign_ups,
            ROW_NUMBER() OVER (
                PARTITION BY JSON_VALUE(data, '$.entity_id'), JSON_VALUE(data, '$.start_time')
                ORDER BY JSON_VALUE(data, '$.end_time') DESC
            ) AS row_number
        FROM `together-internal.spotify_raw.ads_daily_report`
    ) WHERE row_number = 1
),
joint AS (
    SELECT clean_stats.*,ad_adset_campaign.campaign_name,ad_adset_campaign.adset_name FROM clean_stats LEFT JOIN ad_adset_campaign ON clean_stats.ad_id = ad_adset_campaign.ad_id
)SELECT * ,
SPLIT(campaign_name,'_')[OFFSET(1)] AS campaign_descr,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(campaign_name, '_')) >= 7 THEN SPLIT(campaign_name, '_')[OFFSET(7)] 
  ELSE NULL
END AS audience_name,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(ad_name, '_')) >= 7 THEN SPLIT(ad_name, '_')[OFFSET(5)] 
  ELSE NULL
END AS ad_format_detail,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(ad_name, '_')) >= 7 THEN SPLIT(ad_name, '_')[OFFSET(ARRAY_LENGTH(SPLIT(ad_name, '_')) -2 )] 
  ELSE NULL
END AS ad_format,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(ad_name, '_')) >= 7 THEN SPLIT(ad_name, '_')[OFFSET(ARRAY_LENGTH(SPLIT(ad_name, '_')) -1 )] 
  ELSE NULL
END AS creative_descr,
'Spotify' as publisher
FROM joint