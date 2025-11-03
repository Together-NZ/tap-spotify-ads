{{ config(
    materialized='table',
) }}

  WITH ads AS (
        SELECT 
            ad_group_ad_ad_id,
            ad_group_ad_ad_name AS ad_name,
            ad_group_ad_ad_final_urls AS click_url,
            ROW_NUMBER() OVER (PARTITION BY ad_group_ad_ad_id ORDER BY _DATA_DATE DESC) AS row_num
        FROM 
            `together-internal.google_ads_data_transfer.ads_Ad_6544860891`
        WHERE customer_id = 2977297812

    ),
    distinct_ads AS (
        select * from ads where row_num=1
    ),
campaign_data AS (
    SELECT 
        campaign_id,
        campaign_name,
        campaign_campaign_budget AS campaign_budget,
        campaign_status,
        _LATEST_DATE,
        (
        SELECT STRING_AGG(INITCAP(LOWER(part)), ' ')
        FROM UNNEST(SPLIT(campaign_advertising_channel_type, '_')) AS part
    ) AS publisher,
        row_number() over (partition by campaign_id order by _DATA_DATE desc) as row_num
    FROM 
        `together-internal.google_ads_data_transfer.ads_Campaign_6544860891`
    WHERE customer_id = 2977297812
),
distinct_campaign AS (
    select * from campaign_data where row_num=1
),

    ad_group AS (
        SELECT 
            ad_group_id,
            ad_group_name,
            ROW_NUMBER() OVER(PARTITION BY ad_group_id ORDER BY _DATA_DATE desc) AS row_num
        FROM 
            `together-internal.google_ads_data_transfer.ads_AdGroup_6544860891`
        WHERE customer_id = 2977297812
     
        
    ),
    distinct_ad_group as (
      select * from ad_group where row_num=1
    ),
    result_data AS (


SELECT  
    ad_stat.ad_group_ad_ad_id AS ad_id, 
    ad.ad_name,
    ad_group.ad_group_name,
    ad.click_url,
    SUM(ad_stat.metrics_conversions_value) AS revenue, 
    SUM(ad_stat.metrics_conversions) AS total_conversions, 
    SUM(ad_stat.metrics_cost_micros) / 1000000 AS media_cost,
    SUM(ad_stat.metrics_clicks) AS clicks, 
    SUM(ad_stat.metrics_impressions) AS impressions, 
    SUM(ad_stat.metrics_interactions) AS interactions, 
    ad_stat._DATA_DATE as date,
    cam._LATEST_DATE,
    cam.campaign_id AS campaign_external_id,
    cam.campaign_budget AS campaign_budget,
    cam.campaign_name AS campaign_name,
    cam.campaign_status AS campaign_status,
    cam.publisher as publisher,
    ROW_NUMBER() OVER (PARTITION BY ad_stat.ad_group_ad_ad_id,_DATA_DATE ORDER BY ad_stat.ad_group_ad_ad_id,_DATA_DATE) AS row_num  -- Use the array to avoid multiple rows
FROM 
    `together-internal.google_ads_data_transfer.ads_AdBasicStats_6544860891` AS ad_stat
LEFT JOIN 
    distinct_ads AS ad 
ON 
    ad.ad_group_ad_ad_id = ad_stat.ad_group_ad_ad_id
LEFT JOIN 
    distinct_campaign AS cam
ON 
    cam.campaign_id = ad_stat.campaign_id
LEFT JOIN
    distinct_ad_group AS ad_group
ON 
    ad_group.ad_group_id = ad_stat.ad_group_id
where ad_stat.customer_id = 2977297812
GROUP BY 
    _DATA_DATE, ad_id, ad.ad_name, campaign_external_id, campaign_budget, cam.publisher,cam._LATEST_DATE, cam.campaign_status, cam.campaign_name,ad.click_url,ad_group.ad_group_name

    )
SELECT 
    * except(publisher),
    CASE WHEN lower(campaign_name) like '%yt%' THEN 'Youtube'
    WHEN lower(campaign_name) like '%nzme%' THEN 'Nzme'
    WHEN lower(campaign_name) like '%tvnz%' THEN 'Tvnz'
    ELSE
    'Demand Gen' END AS publisher,
    CASE WHEN REGEXP_EXTRACT(ad_group_name, r'PLATFORM_([^_]+)') IS NOT NULL
    THEN REGEXP_EXTRACT(ad_group_name, r'PLATFORM_([^_]+)')
    ELSE ad_group_name END
    AS audience_name,
    'NATIVE' AS media_format,
    CASE WHEN SPLIT(ad_name, '_')[SAFE_OFFSET(5)] IS NOT NULL
    THEN SPLIT(ad_name, '_')[SAFE_OFFSET(5)]
    ELSE ad_name END AS ad_format_detail,
    CASE WHEN SPLIT(ad_name, '_')[SAFE_OFFSET(6)] IS NOT NULL
    THEN SPLIT(ad_name, '_')[SAFE_OFFSET(6)]
    ELSE ad_name END AS ad_format,
    CASE WHEN SPLIT(ad_name, '_')[SAFE_OFFSET(7)] IS NOT NULL
    THEN SPLIT(ad_name, '_')[SAFE_OFFSET(7)]
    ELSE ad_name END AS creative_descr,
    SPLIT(campaign_name,'_')[SAFE_OFFSET(1)] AS campaign_descr
FROM result_data WHERE row_num = 1 and publisher='Demand Gen'

