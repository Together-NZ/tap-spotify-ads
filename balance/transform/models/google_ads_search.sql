{{ config(
    materialized='table',
) }}


WITH campaign_conversion AS (
  SELECT * FROM `together-internal.google_ads_data_transfer.ads_CampaignConversionStats_6544860891`
  WHERE customer_id = 3998638864
),
campaign_basis AS (
  SELECT * FROM `together-internal.google_ads_data_transfer.ads_CampaignBasicStats_6544860891`
  WHERE customer_id = 3998638864
),

campaign AS (
  SELECT * ,
  ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _DATA_DATE DESC) AS row_num FROM `together-internal.google_ads_data_transfer.ads_Campaign_6544860891`
  ),
distinct_campaign AS (
  SELECT * except(row_num) FROM campaign where row_num=1
),
final AS (
  SELECT 
    cam_basis.*,
    cam.* EXCEPT(_LATEST_DATE, _DATA_DATE, campaign_id, customer_id),
   cam_conv.* except (customer_id,campaign_id,campaign_base_campaign,metrics_conversions,metrics_conversions_value,segments_ad_network_type,segments_date,
   segments_slot,_LATEST_DATE,_DATA_DATE),
 metrics_cost_micros / 1000000 AS media_cost,
  metrics_clicks AS clicks,
  metrics_impressions AS impressions,
  cam_basis.segments_date AS date,
  FROM campaign_basis AS cam_basis
  LEFT JOIN distinct_campaign AS cam 
    ON cam.campaign_id = cam_basis.campaign_id
  LEFT JOIN campaign_conversion AS cam_conv
    ON cam_basis.campaign_id = cam_conv.campaign_id
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

FROM result 

WHERE row_num = 1 

)
SELECT *,
CASE WHEN LOWER(publisher) != 'demand gen' THEN campaign_name END AS campaign_name_selection
FROM publisher_data where publisher not like '%Demand Gen%'



