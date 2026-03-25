{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
    FROM `moe-main.ttd_transformed.ttd_transformed`

    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,SAFE_CAST(total_conversions as FLOAT64) as conversions
    
    FROM `moe-main.dv360_transformed.dv360_standard`  WHERE LOWER(campaign_name) NOT LIKE '%yt%'
    UNION ALL
       SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
    
    FROM `moe-main.dv360_transformed.dv360_youtube`  WHERE LOWER(campaign_name) LIKE '%yt%'
    UNION ALL
     SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion
           ,video_25_completion,video_50_completion,video_75_completion, video_play as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
    FROM `moe-main.tiktok_transformed.tiktok`

    UNION ALL

    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_played AS video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,conversions as conversions
    FROM `moe-main.facebook_transformed.facebook` WHERE LOWER(campaign_name) LIKE '%moe%'

    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
    FROM `moe-main.snapchat_transformed.snapchat`
    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_views,
            campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
    FROM `moe-main.linkedin_transformed.linkedin`
    UNION ALL
    select media_cost,impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, 
        CAST(0 AS INT64) AS video_completion,
        CAST(0 AS INT64) AS video_25_completion,
        CAST(0 AS INT64) AS video_50_completion,
        CAST(0 AS INT64) AS video_75_completion,
        CAST(0 AS INT64) AS video_views,
    campaign_name,  publisher, campaign_descr, creative_descr, date(date) as date,null as conversions

from `moe-main.cm360_transformed.cm360_direct_buy`
UNION ALL
       select media_cost,impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, 
        CAST(0 AS INT64) AS video_completion,
        CAST(0 AS INT64) AS video_25_completion,
        CAST(0 AS INT64) AS video_50_completion,
        CAST(0 AS INT64) AS video_75_completion,
        CAST(0 AS INT64) AS video_views,
    campaign_name,  publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
FROM `moe-main.reddit_transformed.reddit`
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
           date,null as conversions
    FROM `moe-main.hivestack_transformed.hivestack`
),
with_channel AS (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel


FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)),
{{dash_table_general_process.dash_table_general_process()}}