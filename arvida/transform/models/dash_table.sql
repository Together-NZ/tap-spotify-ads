{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    SELECT media_cost, impressions,creative_name, clicks,audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
    FROM `arvida-main.ttd_transformed.ttd_transformed`

    UNION ALL

    SELECT media_cost, impressions, creative_name,clicks,  audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, conversions as conversions
    FROM `arvida-main.dv360_transformed.dv360_standard`
    
    UNION ALL
        SELECT media_cost, impressions, creative_name,clicks,  audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
    FROM `arvida-main.dv360_transformed.dv360_youtube`
    UNION ALL


    SELECT media_cost, impressions,
    CAST(ad_name AS STRING) AS creative_name, clicks, 
           CAST(audience_name AS STRING) AS audience_name,
           CAST(ad_format AS STRING) AS ad_format,
           CAST(ad_format_detail AS STRING) AS ad_format_detail, 
            CAST(0 AS INT64) AS video_completion,
            CAST(0 AS INT64) AS video_25_completion,
            CAST(0 AS INT64) AS video_50_completion,
            CAST(0 AS INT64) AS video_75_completion,
            CAST(0 AS INT64) AS video_views,
           
           campaign_name,publisher, campaign_descr, 
           CAST(creative_descr AS STRING) AS creative_descr,
           date,
       conversions as conversions
    FROM `arvida-main.google_ads_display_video_transformed.google_ads_display_videos`
       UNION ALL
                      SELECT media_cost, impressions,
                      ad_name AS creative_name, clicks,
               
           --ARRAY_TO_STRING(media_format, ', ') AS media_format,   -- Convert array to string
           audience_name, -- Convert array to string
           ad_format AS ad_format,         -- Convert array to string
           ad_format_detail AS ad_format_detail, 
            CAST(0 AS INT64) AS video_completion,
            CAST(0 AS INT64) AS video_25_completion,
            CAST(0 AS INT64) AS video_50_completion,
            CAST(0 AS INT64) AS video_75_completion,
            CAST(0 AS INT64) AS video_views,
           
           campaign_name,publisher, campaign_descr, 
            creative_descr,  -- Convert array to string
           date, conversions AS conversions
           FROM `arvida-main.google_ads_search_transformed.google_ads_demand`
    UNION ALL

    SELECT media_cost, impressions,creative_name, CAST(0 AS INT64) AS clicks, 
           --media_format AS media_format,   -- Convert array to string
           audience_name AS audience_name, -- Convert array to string
           ad_format AS ad_format,         -- Convert array to string
           ad_format_detail AS ad_format_detail, 
            CAST(0 AS INT64) AS video_completion,
            CAST(0 AS INT64) AS video_25_completion,
            CAST(0 AS INT64) AS video_50_completion,
            CAST(0 AS INT64) AS video_75_completion,
            CAST(0 AS INT64) AS video_views,
           
           campaign_name,publisher, campaign_descr, 
           creative_descr AS creative_descr, -- Convert array to string
           date,
           null as conversions
    FROM `arvida-main.hivestack_transformed.hivestack`

    UNION ALL
    SELECT media_cost, impressions, creative_name,clicks,audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_played AS video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, conversions as conversions
    FROM `arvida-main.facebook_transformed.facebook`

    UNION ALL
    select media_cost,impressions, creative_name,clicks, audience_name, ad_format, ad_format_detail, 
        CAST(0 AS INT64) AS video_completion,
        CAST(0 AS INT64) AS video_25_completion,
        CAST(0 AS INT64) AS video_50_completion,
        CAST(0 AS INT64) AS video_75_completion,
        CAST(0 AS INT64) AS video_views,
    campaign_name,  publisher, campaign_descr, creative_descr, date(date) as date,
    null as conversions
from `arvida-main.cm360_transformed.cm360_direct_buy`
),
with_channel as (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)
where lower(campaign_name) like '%arv%'
),
{{dash_table_general_process.dash_table_general_process()}}