{{ config(
    materialized='table',
) }}
WITH ads AS (
  SELECT
    ad_group_ad_ad_id,
    ad_group_ad_ad_name AS ad_name,
    ad_group_ad_ad_final_urls AS click_url,
    ROW_NUMBER() OVER (
      PARTITION BY ad_group_ad_ad_id
      ORDER BY _DATA_DATE DESC
    ) AS row_num
  FROM `together-internal.google_ads_data_transfer.ads_Ad_6544860891`
  WHERE customer_id = 4714069556
),
distinct_ads AS (
  SELECT * FROM ads WHERE row_num = 1
),
campaign_data AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY campaign_id
      ORDER BY _DATA_DATE DESC
    ) AS row_num
  FROM `together-internal.google_ads_data_transfer.ads_Campaign_6544860891`
  WHERE customer_id = 4714069556
),
ad_data as (
    SELECT
    ad_stat.ad_group_ad_ad_id AS ad_id,
    SUM(ad_stat.metrics_conversions_value) AS revenue,
    SUM(ad_stat.metrics_conversions) AS total_conversions,
    SUM(ad_stat.metrics_cost_micros) / 1000000 AS media_cost,
    SUM(ad_stat.metrics_clicks) AS clicks,
    SUM(ad_stat.metrics_impressions) AS impressions,
    SUM(ad_stat.metrics_interactions) AS interactions,
    ad_stat._DATA_DATE AS _DATA_DATE,
    ad_stat.campaign_id,
    ad_stat.segments_device
  FROM `together-internal.google_ads_data_transfer.ads_AdBasicStats_6544860891` AS ad_stat where customer_id=4714069556 
  GROUP BY ad_stat.campaign_id,ad_stat.segments_device,ad_stat.ad_group_ad_ad_id,ad_stat._DATA_DATE
) ,
video AS (
SELECT
    campaign_id,
    ad_group_ad_ad_id as ad_id,segments_device,
    _DATA_DATE,
    metrics_video_quartile_p50_rate,
    metrics_video_quartile_p25_rate,
    metrics_video_quartile_p75_rate,
    metrics_video_quartile_p100_rate,
    metrics_video_views,
    ROW_NUMBER() OVER (
      PARTITION BY campaign_id, ad_group_ad_ad_id, _DATA_DATE
      ORDER BY campaign_id, ad_group_ad_ad_id, _DATA_DATE
    ) AS row_num
  FROM `together-internal.google_ads_data_transfer.ads_VideoNonClickStats_6544860891`
  WHERE customer_id = 4714069556 ),
  video_metrics as (
    SELECT 
    ad_stat.ad_id ,
    ad_stat.campaign_id,
    ad_stat._DATA_DATE AS date,
    ad_stat.impressions * vd.metrics_video_quartile_p50_rate AS video_50_completion,
    ad_stat.impressions * vd.metrics_video_quartile_p25_rate AS video_25_completion,
    ad_stat.impressions * vd.metrics_video_quartile_p75_rate AS video_75_completion,
    ad_stat.impressions * vd.metrics_video_quartile_p100_rate AS video_completion,
    ad_stat.impressions * vd.metrics_video_views AS video_views
    FROM ad_data as ad_stat
    LEFT JOIN video as vd
    ON ad_stat.ad_id = vd.ad_id
    AND ad_stat.campaign_id = vd.campaign_id
    AND ad_stat.segments_device = vd.segments_device
    AND ad_stat._DATA_DATE = vd._DATA_DATE
  ),
  aggregrated_video_metrics AS (
    SELECT SUM(video_50_completion) AS video_50_completion,
    SUM(video_25_completion) AS video_25_completion,
    SUM(video_75_completion) AS video_75_completion,
    SUM(video_completion) AS video_completion,
    SUM(video_views) AS video_views,
    campaign_id,
    ad_id,
    date
    FROM video_metrics
    GROUP BY ad_id,campaign_id,date
  ),
distinct_campaign AS (
  SELECT * EXCEPT(row_num, _DATA_DATE)
  FROM campaign_data
  WHERE row_num = 1
),
ad_group AS (
  SELECT
    ad_group_id,
    ad_group_name,
    ROW_NUMBER() OVER (
      PARTITION BY ad_group_id
      ORDER BY _DATA_DATE DESC
    ) AS row_num
  FROM `together-internal.google_ads_data_transfer.ads_AdGroup_6544860891`
  WHERE customer_id = 4714069556  -- <-- adjust if 1287289620 was intentional
),
distinct_ad_group AS (
  SELECT * FROM ad_group WHERE row_num = 1
),
-- do the aggregation in a subquery, window calc outside of it later if needed
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
    ad_stat._DATA_DATE AS date,
    cam._LATEST_DATE,
    cam.campaign_id AS campaign_external_id,
    cam.campaign_campaign_budget AS campaign_budget,
    cam.campaign_name AS campaign_name,
    cam.campaign_status AS campaign_status
  FROM `together-internal.google_ads_data_transfer.ads_AdBasicStats_6544860891` AS ad_stat
  LEFT JOIN distinct_ads         AS ad       ON ad.ad_group_ad_ad_id = ad_stat.ad_group_ad_ad_id
  LEFT JOIN distinct_campaign    AS cam      ON cam.campaign_id      = ad_stat.campaign_id
  LEFT JOIN distinct_ad_group    AS ad_group ON ad_group.ad_group_id = ad_stat.ad_group_id

                                                   
  WHERE ad_stat.customer_id = 4714069556
  GROUP BY
    ad_stat._DATA_DATE,  -- fully qualified to be explicit
    ad_id,
    ad.ad_name,
    campaign_external_id,
    campaign_budget,
    cam._LATEST_DATE,
    cam.campaign_status,
    cam.campaign_name,
    ad.click_url,
    ad_group.ad_group_name
),
unmerged_campaigns AS (
  SELECT
    *,
    CASE
      WHEN LOWER(campaign_name) LIKE '%yt%'   THEN 'Youtube'
      WHEN LOWER(campaign_name) LIKE '%nzme%' THEN 'Nzme'
      WHEN LOWER(campaign_name) LIKE '%tvnz%' THEN 'Tvnz'
      ELSE 'Demand Gen'
    END AS publisher,
    CASE
      WHEN REGEXP_EXTRACT(ad_group_name, r'PLATFORM_([^_]+)') IS NOT NULL
        THEN REGEXP_EXTRACT(ad_group_name, r'PLATFORM_([^_]+)')
      ELSE ad_group_name
    END AS audience_name,
    CASE
      WHEN SPLIT(ad_name, '_')[SAFE_OFFSET(5)] IS NOT NULL
        THEN SPLIT(ad_name, '_')[SAFE_OFFSET(5)]
      ELSE ad_name
    END AS ad_format_detail,
    CASE
      WHEN SPLIT(ad_name, '_')[SAFE_OFFSET(6)] IS NOT NULL
        THEN SPLIT(ad_name, '_')[SAFE_OFFSET(6)]
      ELSE ad_name
    END AS ad_format,
    CASE
      WHEN SPLIT(ad_name, '_')[SAFE_OFFSET(7)] IS NOT NULL
        THEN SPLIT(ad_name, '_')[SAFE_OFFSET(7)]
      ELSE ad_name
    END AS creative_descr,
    SPLIT(campaign_name, '_')[SAFE_OFFSET(1)] AS campaign_descr,
    -- if you still need row_num per day/ad, add it here safely:
    ROW_NUMBER() OVER (
      PARTITION BY ad_id, date
      ORDER BY ad_id, date
    ) AS row_num
  FROM result_data
  -- if you want just the first row per (ad_id, _DATA_DATE), you can filter after adding row_num
),without_video AS (
SELECT
  * EXCEPT(campaign_name),
  CASE
    WHEN campaign_name = 'PBT0001_Expect the Expected_GOOGLEADS_YT_AWARENESS_Non Skippable (30")'
      THEN 'PBT0001_Expect the Expected_GOOGLEADS_YT_AWARENESS_Building Brand'
    ELSE campaign_name
  END AS campaign_name
FROM unmerged_campaigns)
SELECT wvd.* EXCEPT(date) ,
vd.* EXCEPT(ad_id,video_views,video_25_completion,video_50_completion,video_75_completion,video_completion),
CASE WHEN video_views IS NULL THEN 0 ELSE video_views END AS video_views,
CASE WHEN video_25_completion IS NULL THEN 0 ELSE ROUND(video_25_completion) END AS video_25_completion,
CASE WHEN video_50_completion IS NULL THEN 0 ELSE ROUND(video_50_completion) END AS video_50_completion,
CASE WHEN video_75_completion IS NULL THEN 0 ELSE ROUND(video_75_completion) END AS video_75_completion,
CASE WHEN video_completion IS NULL THEN 0 ELSE ROUND(video_completion) END AS video_completion
FROM without_video AS wvd 
LEFT JOIN aggregrated_video_metrics as vd 
ON wvd.date = vd.date AND wvd.ad_id=vd.ad_id 