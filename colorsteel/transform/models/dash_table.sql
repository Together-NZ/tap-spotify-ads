{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion, video_25_completion, video_50_completion, video_75_completion, video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
    FROM `colorsteel-main.ttd_transformed.ttd_transformed`
    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail,
        CAST(0 AS INT64) AS video_completion,
        CAST(0 AS INT64) AS video_25_completion,
        CAST(0 AS INT64) AS video_50_completion,
        CAST(0 AS INT64) AS video_75_completion,
        CAST(0 AS INT64) AS video_views,
        campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
    FROM `colorsteel-main.cm360_transformed.cm360_direct_buy`
    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion, video_25_completion, video_50_completion, video_75_completion, video_played AS video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, conversions
    FROM `colorsteel-main.facebook_transformed.facebook`
    UNION ALL
    SELECT safe_cast(media_cost as float64), safe_cast(impressions as int64), SAFE_CAST(clicks AS INT64), creative_name, audience_name, ad_format, ad_format_detail, safe_cast(video_completion as int64), safe_cast(video_25_completion as int64), SAFE_CAST(video_50_completion AS INT64), SAFE_CAST(video_75_completion AS INT64), SAFE_CAST(video_views AS INT64),
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, null as conversions
    FROM `colorsteel-main.pinterest_transformed.pinterest`
    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion, video_25_completion, video_50_completion, video_75_completion, video_25_completion AS video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, conversions
    FROM `colorsteel-main.dv360_transformed.dv360_standard` WHERE LOWER(campaign_name) NOT LIKE '%yt%'
    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion, video_25_completion, video_50_completion, video_75_completion, video_25_completion AS video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date, conversions
    FROM `colorsteel-main.dv360_transformed.dv360_youtube`
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel
FROM dash_table as dt LEFT JOIN `together-internal.channel.publisher_channel` as dc
ON lower(trim(dt.publisher)) = lower(trim(dc.publisher))
),
{{dash_table_general_process.dash_table_general_process()}}
