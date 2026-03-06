{{ config(
    materialized='table',
) }}
WITH 
data AS (
  select json_value(data,'$.AD_ID') as ad_id,
  json_value(data,'$.CAMPAIGN_ID') as campaign_id,
  json_value(data,'$.DATE') as date,
  json_value(data,'$.IMPRESSION_1') as impressions,
  json_value(data,'$.OUTBOUND_CLICK_1') as clicks,
  json_value(data,'$.SPEND_IN_DOLLAR') as media_cost,
  json_value(data,'$.TOTAL_VIDEO_P100_COMPLETE') as video_completion,
  JSON_VALUE(data,'$.TOTAL_VIDEO_P25_COMBINED') as video_25_completion,
  json_value(data,'$.TOTAL_VIDEO_P50_COMBINED') as video_50_completion,
  json_value(data,'$.TOTAL_VIDEO_P75_COMBINED') as video_75_completion,
  json_value(data,'$.TOTAL_VIDEO_MRC_VIEWS') as video_views,
  ROW_NUMBER() OVER (PARTITION BY json_value(data,'$.AD_ID') ,
  json_value(data,'$.AD_ID'),json_value(data,'$.DATE') ORDER BY _sdc_extracted_at DESC ) as row_num,

  from `colorsteel-main.pinterest_raw.reports`
),
ad_group as (
  SELECT DISTINCT JSON_VALUE(data,'$.id') as ad_group_id,
  JSON_VALUE(data,'$.name') as ad_group_name
  FROM `colorsteel-main.pinterest_raw.ad_groups`
),
campaign as (
  select DISTINCT JSON_VALUE(data,'$.id') as campaign_id,
   JSON_VALUE(data,'$.name') as campaign_name
  FROM `colorsteel-main.pinterest_raw.campaigns`
  WHERE LOWER(JSON_VALUE(data,'$.name')) LIKE '%colorsteel%'
),
deduplicate_data as (
  select * from data where row_num = 1
),
ad as (
    select DISTINCT JSON_VALUE(data,'$.id') as ad_id,
   JSON_VALUE(data,'$.name') as creative_name,
   JSON_VALUE(data,'$.ad_group_id') as ad_group_id
  FROM `colorsteel-main.pinterest_raw.ads`
),

sub_result as (
select deduplicate_data.*,
ad.* except(ad_id),
campaign.* except(campaign_id)
FROM  deduplicate_data left join ad on deduplicate_data.ad_id = ad.ad_id LEFT JOIN campaign on 
deduplicate_data.campaign_id = campaign.campaign_id),
raw_final as (
SELECT sub_result.* ,
ad_group.* except(ad_group_id)FROM sub_result as sub_result
 LEFT JOIN ad_group on sub_result.ad_group_id = ad_group.ad_group_id)
SELECT *,
CASE WHEN ARRAY_LENGTH(SPLIT(ad_group_name, '_'))>=8 THEN
SPLIT(ad_group_name, '_')[OFFSET(7)] 
ELSE NULL
END AS audience_name,
CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) <8 THEN 'Other' ELSE SPLIT(creative_name, '_')[OFFSET(5)] END AS ad_format_detail,
CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) <8 THEN 'Other' ELSE SPLIT(creative_name, '_')[OFFSET(6)] END AS ad_format,
CASE WHEN ARRAY_LENGTH(SPLIT(creative_name, '_')) <8 THEN 'Other' ELSE SPLIT(creative_name, '_')[OFFSET(7)]END AS creative_descr,
CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) <=1 THEN 'Other' ELSE SPLIT(campaign_name,'_')[OFFSET(1)] END AS campaign_descr,
'Pinterest' AS publisher
FROM raw_final