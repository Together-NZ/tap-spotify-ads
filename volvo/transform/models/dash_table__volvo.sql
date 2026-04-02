{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    SELECT media_cost, impressions, clicks,creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
    FROM `volvo-main.ttd_transformed__volvo.ttd_transformed__volvo`

    UNION ALL
    SELECT media_cost, impressions, clicks,creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, conversions
    FROM `volvo-main.dv360_transformed__volvo.dv360_youtube__volvo` WHERE LOWER(campaign_name) NOT LIKE '%yt%'
    UNION ALL
        SELECT media_cost, impressions, clicks,creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
    FROM `volvo-main.linkedin_transformed__volvo.linkedin__volvo`
    UNION ALL

    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, conversions
    FROM `volvo-main.dv360_transformed__volvo.dv360_standard__volvo` 
    UNION ALL
           SELECT media_cost, impressions,clicks,
              ad_name AS creative_name,  
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
           date, conversions

    FROM `volvo-main.google_ads_search_transformed__volvo.google_ads_demand__volvo`
    
    UNION ALL
        SELECT media_cost, impressions, CAST(0 AS INT64) AS clicks, 
           creative_name,   -- Convert array to string
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
           date, null as conversions
    FROM `volvo-main.hivestack_transformed__volvo.hivestack__volvo`
    UNION ALL
           SELECT media_cost, impressions,clicks,
              ad_name AS creative_name,  
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
           date, conversions

    FROM `volvo-main.google_ads_search_transformed__volvo.google_ads_demand_2__volvo`
    UNION ALL
    -- Handling Google Ads arrays by converting them to strings

    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_played AS video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,conversions
    FROM `volvo-main.facebook_transformed__volvo.facebook__volvo`

    UNION ALL
    select media_cost,impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, 
        CAST(0 AS INT64) AS video_completion,
        CAST(0 AS INT64) AS video_25_completion,
        CAST(0 AS INT64) AS video_50_completion,
        CAST(0 AS INT64) AS video_75_completion,
        CAST(0 AS INT64) AS video_views,
    campaign_name,  publisher, campaign_descr, creative_descr, date(date) as date,
        null as conversions
from `volvo-main.cm360_transformed__volvo.cm360_direct_buy__volvo`
),
with_channel AS (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)

),
{{dash_table_general_process.dash_table_general_process()}}