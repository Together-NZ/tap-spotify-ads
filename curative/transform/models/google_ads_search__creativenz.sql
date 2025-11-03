{{ config(
    materialized='table',
) }}

WITH campaign_conversion AS (
  SELECT * FROM `together-internal.google_ads_data_transfer.ads_CampaignConversionStats_6544860891`
  WHERE customer_id = 4765933337
),
campaign_basis AS (
  SELECT * FROM `together-internal.google_ads_data_transfer.ads_CampaignBasicStats_6544860891`
  WHERE customer_id = 4765933337
),
campaign AS (
  SELECT * FROM `together-internal.google_ads_data_transfer.ads_Campaign_6544860891`
  WHERE customer_id = 4765933337
),
final AS (
  SELECT 
    cam_cov.* EXCEPT(customer_id, _LATEST_DATE, campaign_base_campaign, metrics_conversions, metrics_conversions_value, _DATA_DATE, campaign_id, segments_slot, segments_ad_network_type, segments_date),
    cam.* EXCEPT(_LATEST_DATE, _DATA_DATE, campaign_id, customer_id),
    cam_basis.* EXCEPT( metrics_clicks,metrics_impressions,metrics_cost_micros,segments_date),
 metrics_cost_micros / 1000000 AS media_cost,
  metrics_clicks AS clicks,
  metrics_impressions AS impressions,
  cam_basis.segments_date AS date,
  FROM campaign_conversion AS cam_cov
  LEFT JOIN campaign_basis AS cam_basis 
    ON cam_cov.campaign_id = cam_basis.campaign_id
  LEFT JOIN campaign AS cam 
    ON cam.campaign_id = cam_basis.campaign_id
),
result AS (
  SELECT *, 
         ROW_NUMBER() OVER (
           PARTITION BY campaign_name, date, clicks, impressions,  bidding_strategy_name
         ) AS row_num 
  FROM final
),
customer AS (
  SELECT *  
  FROM `together-internal.google_ads_data_transfer.ads_Customer_6544860891`
  WHERE customer_id = 4765933337
)

SELECT 
  *,
  campaign_name AS campaign_name_selection,
  
  -- Capitalize each word split by "_" in `campaign_advertising_channel_type`
  (
    SELECT STRING_AGG(INITCAP(LOWER(part)), ' ')
    FROM UNNEST(SPLIT(campaign_advertising_channel_type, '_')) AS part
  ) AS publisher

FROM result 
WHERE row_num = 1




