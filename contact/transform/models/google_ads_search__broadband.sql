{{ config(
    materialized='table',
) }}
WITH campaign_conversion AS (
  SELECT * FROM `contact-energy-main.google_ads_search_raw.ads_CampaignConversionStats_4362586476`
),
campaign_basis AS (
  SELECT * FROM `contact-energy-main.google_ads_search_raw.ads_CampaignBasicStats_4362586476`

),

campaign AS (
  SELECT * ,
  ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _DATA_DATE DESC) AS row_num FROM `contact-energy-main.google_ads_search_raw.ads_Campaign_4362586476`

  ),
distinct_campaign AS (
  SELECT * except(row_num) FROM campaign where row_num=1
),
final AS (
  SELECT 
    cam_basis.* except(metrics_cost_micros,metrics_clicks,metrics_impressions),
    cam.* except(campaign_id,customer_id,_LATEST_DATE,_DATA_DATE),
 cam_basis.metrics_cost_micros / 1000000 AS media_cost,
  cam_basis.metrics_clicks AS clicks,
  cam_basis.metrics_impressions AS impressions,
  cam_basis.segments_date AS date,
  FROM campaign_basis AS cam_basis
  LEFT JOIN distinct_campaign AS cam 
    ON cam.campaign_id = cam_basis.campaign_id
),
result AS (
  SELECT *, 
         ROW_NUMBER() OVER (
           PARTITION BY campaign_name, date, clicks, impressions,  bidding_strategy_name
         ) AS row_num 
  FROM final

),
publisher_data AS (

SELECT 
  *,
  
  -- Capitalize each word split by "_" in `campaign_advertising_channel_type`
  (
    SELECT STRING_AGG(INITCAP(LOWER(part)), ' ')
    FROM UNNEST(SPLIT(campaign_advertising_channel_type, '_')) AS part
  ) AS publisher

FROM result  WHERE ( lower(campaign_name) like '%broadband%') 
)



SELECT *,
CASE WHEN LOWER(publisher) != 'demand gen' THEN campaign_name END AS campaign_name_selection
FROM publisher_data where publisher not like '%Demand Gen%'













