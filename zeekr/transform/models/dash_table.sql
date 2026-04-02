{{ config(
    materialized='table',
) }}

with dash_table AS (
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
           date,
       conversions
    FROM `zeekr-main.google_ads_search_transformed.google_ads_demand`
    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name,audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_played AS video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,conversions
    FROM `zeekr-main.facebook_transformed.facebook`
    UNION ALL
    SELECT media_cost, impressions, clicks,creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
    FROM `zeekr-main.ttd_transformed.ttd`
    UNION ALL
    SELECT media_cost, impressions,  clicks,creative_name,audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_views,
            campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
    FROM `zeekr-main.linkedin_transformed.linkedin`
),
with_channel AS (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)

),
{{dash_table_general_process.dash_table_general_process()}}