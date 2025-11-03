{{ config(
    materialized='table',
) }}
WITH aggregated_ttd AS (
    SELECT 
        ad_server_creative_placement_id,
        ad_server_name,
        date, -- Keep this as-is for joining purposes
        campaign_name,
        campaign_id,
        creative,
        creative_id,
        advertiser,
        advertiser_id,
        publisher,
        media_format,
        audience_name,
        ad_format,
        ad_format_detail,
        creative_descr,
        campaign_descr,
        SUM(player_25_complete) AS player_25_complete,
        SUM(player_50_complete) AS player_50_Complete,
        SUM(player_75_complete) AS player_75_complete,
        SUM(player_100_complete) AS video_completion,
        SUM(clicks) AS clicks,
        SUM(video_views) AS video_views,
        SUM(impressions) AS impressions,
        SUM(partner_cost_partner_currency) AS media_cost -- Aggregate Partner Cost
    FROM {{ref('ttd_seralized')}}
    GROUP BY 
        ad_server_creative_placement_id, ad_server_name, date, campaign_name, campaign_id, creative, creative_id, advertiser, advertiser_id,publisher,media_format,
        audience_name,ad_format,ad_format_detail,creative_descr,campaign_descr
),
cm360 AS (
    SELECT 
        placement_id, 
        date AS cm360_date, -- Extract date for grouping
        SUM(impressions) AS cm360_impressions, 
        SUM(clicks) AS cm360_clicks,
        ROW_NUMBER() OVER (
            PARTITION BY placement_id,date -- Ensure DATE(date) matches the GROUP BY clause
          
        ) AS row_num
    FROM `uowaikato-main.cm360_transformed.cm360_seralized`
    GROUP BY 
        placement_id, date -- Ensure consistency
),
filtered_cm360 AS (
    SELECT 
        placement_id, 
        cm360_date,
        cm360_impressions, 
        cm360_clicks
    FROM cm360
    WHERE row_num = 1 -- Keep only the top-ranked row for each partition
),
joined_data AS (
    SELECT
        ttd.ad_server_creative_placement_id,
        ttd.ad_server_name,
        safe_cast(ttd.player_25_complete as INT64) as video_25_completion,
        safe_cast(ttd.player_50_complete as INT64) as video_50_completion,
        safe_cast(ttd.player_75_complete as INT64) as video_75_completion,
        safe_cast(ttd.video_completion as INT64) as video_completion, 
        safe_cast(ttd.video_views as INT64) as video_views,
        ttd.date,
        ttd.campaign_name as campaign_name,
        ttd.campaign_id,
        ttd.creative as creative_name,
        ttd.creative_id,
        ttd.advertiser,
        ttd.advertiser_id,
        ttd.media_cost,
        ttd.publisher,
        ttd.media_format,
        ttd.audience_name,
        ttd.ad_format,
        ttd.ad_format_detail,
        ttd.creative_descr,
        ttd.campaign_descr,
        -- replce click and impressions from ttd from cm360 with matching adserver placement id from ttd and placement id from cm360
        COALESCE(cm.cm360_clicks, ttd.clicks) AS clicks,
        COALESCE(cm.cm360_impressions, ttd.impressions) AS impressions
    FROM 
        aggregated_ttd ttd
    LEFT JOIN 
        filtered_cm360 cm
    ON 
        ttd.ad_server_creative_placement_id = cm.placement_id
        AND Date(ttd.date) = cm.cm360_date -- Join on the exact match of Date and CM360_Date
)
SELECT * 
FROM joined_data
