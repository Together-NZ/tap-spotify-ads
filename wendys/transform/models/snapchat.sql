{{ config(
    materialized='table',
) }}

WITH ad_stat_filtered_duplicate AS (
    SELECT 
    -- vidoe metrics, actions(like, comment and etc)
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id, ad_start_time
            ORDER BY extraction_date DESC
        ) AS row_num
    FROM (
        SELECT
            _sdc_extracted_at AS extraction_date,
            JSON_EXTRACT_SCALAR(data, "$.id") AS id,
            DATETIME(TIMESTAMP(JSON_EXTRACT_SCALAR(data, "$.start_time")),"Pacific/Auckland") AS ad_start_time,
            JSON_EXTRACT_SCALAR(data, "$.impressions") AS impressions,
            SAFE_CAST(JSON_EXTRACT_SCALAR(data, "$.spend")as float64)  AS spend,
            JSON_EXTRACT_SCALAR(data, "$.quartile_1") AS quartile_1,
            JSON_EXTRACT_SCALAR(data, "$.quartile_2") AS quartile_2,
            JSON_EXTRACT_SCALAR(data, "$.quartile_3") AS quartile_3,
            JSON_EXTRACT_SCALAR(data, "$.view_completion") AS view_completion,
            JSON_EXTRACT_SCALAR(data, "$.frequency") AS frequency,
            JSON_EXTRACT_SCALAR(data, "$.uniques") AS unqieus,
            JSON_EXTRACT_SCALAR(data, "$.swipes") AS clicks,
            JSON_EXTRACT_SCALAR(data, "$.video_views") AS video_views,
            JSON_EXTRACT_SCALAR(data, "$.view_time_millis") AS view_time_millis
        FROM `wendys-main.snapchat_raw.ad_stats_daily`
    )
  
),
ad_stat_filtered AS (
    select * from ad_stat_filtered_duplicate where row_num=1
),

ad_details AS (
    SELECT 
    -- ad, media_buy and campaign advertiser
        JSON_EXTRACT_SCALAR(data, "$.id") AS id,
        JSON_EXTRACT_SCALAR(data, "$.name") AS ad_name,
        JSON_EXTRACT_SCALAR(data, "$.type") AS ad_type,
        JSON_EXTRACT_SCALAR(data, "$.updated_at") AS ad_updated_at,
        JSON_EXTRACT_SCALAR(data, "$.created_at") AS ad_created_at,
        JSON_EXTRACT_SCALAR(data, "$.ad_squad_id") AS media_buy_external_id,
        JSON_EXTRACT_SCALAR(data, "$.ad_account_id") AS campaign_advertiser_id,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(data, "$.id") ORDER BY _sdc_extracted_at)as row_num
    FROM `wendys-main.snapchat_raw.ads`
),
ad_details_filtered AS (
  SELECT * FROM ad_details where row_num=1
),

ad_squad AS (
    SELECT 
    -- ad squad, media buy name , campaign id and cost model
        JSON_EXTRACT_SCALAR(data, "$.id") AS id,
        JSON_EXTRACT_SCALAR(data, "$.name") AS media_buy_name,
        JSON_EXTRACT_SCALAR(data, "$.type") AS ad_squad_type,
        JSON_EXTRACT_SCALAR(data, "$.optimization_goal") AS media_buy_cost_model,
        JSON_EXTRACT_SCALAR(data, "$.campaign_id") AS campaign_id,
        ROW_NUMBER() OVER(PARTITION BY JSON_EXTRACT_SCALAR(data, "$.id"),JSON_EXTRACT_SCALAR(data, "$.campaign_id"),JSON_EXTRACT_SCALAR(data, "$.name"),JSON_EXTRACT_SCALAR(data, "$.type") ORDER BY _sdc_extracted_at) as row_num

    FROM `wendys-main.snapchat_raw.ad_squads`
),
ad_squad_filtered AS (
  SELECT *  FROM ad_squad where row_num=1
),

campaign AS (
    SELECT 
    -- campaign details ( id, name , status and etc)
        JSON_EXTRACT_SCALAR(data, "$.id") AS campaign_id,
        JSON_EXTRACT_SCALAR(data, "$.name") AS campaign_name,
        JSON_EXTRACT_SCALAR(data, "$.status") AS campaign_status,
        JSON_EXTRACT_SCALAR(data, "$.start_time") AS campaign_start_time,
        JSON_EXTRACT_SCALAR(data, "$.end_time") AS campaign_end_time,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(data, "$.id"),JSON_EXTRACT_SCALAR(data, "$.name") ORDER BY _sdc_extracted_at ) as row_num
    FROM `wendys-main.snapchat_raw.campaigns`
),
campaign_filtered AS (
    SELECT * FROM campaign WHERE row_num=1
),

combined_data AS (
    SELECT 
    -- combined data
        ad_stat.id AS ad_key,
        ad_stat.ad_start_time,
        ad_stat.impressions,
        ad_stat.spend as media_cost,
        ad_stat.quartile_1,
        ad_stat.quartile_2,
        ad_stat.quartile_3,
        ad_stat.view_completion,
        ad_stat.frequency,
        ad_stat.clicks,
        ad_stat.video_views,
        ad_stat.view_time_millis,
        ad_details.ad_name,
        ad_details.ad_type,
        ad_details.ad_updated_at,
        ad_details.ad_created_at,
        ad_details.media_buy_external_id,
        ad_details.campaign_advertiser_id,
        ad_squad.media_buy_name,
        ad_squad.ad_squad_type,
        ad_squad.media_buy_cost_model,
        campaign.campaign_id,
        campaign.campaign_name,
        campaign.campaign_status,
        campaign.campaign_start_time,
        campaign.campaign_end_time
    FROM ad_stat_filtered ad_stat
    LEFT JOIN ad_details_filtered ad_details ON ad_stat.id = ad_details.id
    LEFT JOIN ad_squad_filtered ad_squad ON ad_details.media_buy_external_id = ad_squad.id
    LEFT JOIN campaign_filtered campaign ON ad_squad.campaign_id = campaign.campaign_id
),
final AS (

-- Step 6: Aggregate and finalize data
SELECT 
    ad_start_time AS date,
    ad_key,
    ad_name,
    ad_type,
    SUM(CAST(quartile_1 AS INT64)) AS video_25_completion,
    SUM(CAST(quartile_2 AS INT64)) AS video_50_completion,
    SUM(CAST(quartile_3 AS INT64)) AS video_75_completion,
    SUM(CAST(view_completion AS INT64)) AS video_completion,
    AVG(CAST(frequency AS FLOAT64)) AS frequency,
    SUM(CAST(media_cost AS FLOAT64) / 1000000) AS media_cost,
    SUM(CAST(clicks AS INT64)) AS clicks,
    SUM(CAST(impressions AS INT64)) AS impressions,
    SUM(CAST(video_views AS INT64)) AS video_views,
    SUM(CAST(view_time_millis AS FLOAT64) / 1000000) AS total_view,
    ad_updated_at,
    ad_created_at,
    media_buy_external_id,
    media_buy_name,
    media_buy_cost_model,
    ad_squad_type,
    campaign_advertiser_id,
    campaign_start_time,
    campaign_end_time,
    campaign_id,
    campaign_name,
    campaign_status,
    ROW_NUMBER() OVER (PARTITION BY ad_start_time, ad_key, campaign_name) AS row_num
FROM 
    combined_data
GROUP BY 
    ad_start_time, ad_key, ad_name, ad_type, ad_updated_at, ad_created_at, media_buy_external_id, 
    media_buy_name, ad_squad_type, campaign_advertiser_id, campaign_start_time, campaign_end_time,
    campaign_id, campaign_name, campaign_status, media_buy_cost_model)

SELECT * EXCEPT(ad_name), ad_name as creative_name,
    'Snapchat' AS publisher,
    REGEXP_EXTRACT(media_buy_name, r'PLATFORM_([^_]+)') AS audience_name,
    CASE 
        WHEN SPLIT (ad_name,'_')[OFFSET(1)] LIKE 'SOCIAL%'
        AND (
            lower(media_buy_name) LIKE '%vid%'
            OR lower(ad_name) LIKE '%vid%'
            OR lower(campaign_name) LIKE '%vid%'
        ) THEN 'Social Video'
        WHEN SPLIT (ad_name,'_')[OFFSET(1)] LIKE 'SOCIAL%'
        AND (
            lower(media_buy_name) NOT LIKE '%vid%'
            AND lower(ad_name) NOT LIKE '%vid%'
            AND lower(campaign_name) NOT LIKE '%vid%'
        )
        THEN 'Social Display'
        ELSE 'Other'
    END AS media_format,
    SPLIT(ad_name, '_')[OFFSET(5)] AS ad_format_detail,
    SPLIT(ad_name, '_')[OFFSET(6)] AS ad_format,
    SPLIT(ad_name, '_')[OFFSET(7)] AS creative_descr,
    CASE 
        WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=2 THEN
        SPLIT(campaign_name,'_')[OFFSET(1)] 
        ELSE 'Other'
    END AS campaign_descr

FROM final WHERE row_num = 1
