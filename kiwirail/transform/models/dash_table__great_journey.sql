{{ config(
    materialized='table',
) }}
WITH dash_table AS (


    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_played AS video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,conversions AS conversions
    FROM `kiwirail-main.facebook_transformed__great_journey.facebook__great_journey`


    UNION ALL

    SELECT media_cost, impressions, clicks, creative_name,audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,NULL AS conversions
    FROM `kiwirail-main.dv360_transformed__great_journey.dv360_standard__great_journey` WHERE LOWER(campaign_name) not like '%yt%'
    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name,audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,NULL AS conversions
    FROM `kiwirail-main.dv360_transformed__great_journey.dv360_youtube__great_journey`
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
           date, conversions AS conversions

    FROM `kiwirail-main.google_ads_search_transformed__great_journey.google_ads_demand__great_journey`
),
with_channel as (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)
where lower(campaign_name) like '%gjnz%'),
{{dash_table_general_process.dash_table_general_process()}}