{{ config(
    materialized='table',
) }}
WITH ad_data as (
    SELECT 
    JSON_VALUE(data,'$.ad_id') as ad_id,
    JSON_VALUE(data,'$.campaign_name') as campaign_name,
    JSON_VALUE(data,'$.campaign_id') as campaign_id,
    JSON_VALUE(data,'$.metrics.clicks') as clicks,
    JSON_VALUE(data,'$.metrics.impressions') as impressions,
    JSON_VALUE(data,'$.metrics.spend') as media_cost,
    JSON_VALUE(data,'$.date') as date,
    _sdc_extracted_at ,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.ad_id'),JSON_VALUE(data,'$.campaign_id'),JSON_VALUE(data,'$.date') order by _sdc_extracted_at DESC ) as row_num
    
    from `aia-nz-main.outbrain_raw.ad_report`
),
deduplicate_data AS (
  select *
  from ad_data
  WHERE row_num = 1
  
),
ad AS (
    SELECT DISTINCT
    JSON_VALUE(data,'$.id') as ad_id,
    JSON_VALUE(data,'$.text') as ad_name
    FROM `aia-nz-main.outbrain_raw.ad`
)

SELECT * except(ad_id,_sdc_extracted_at,row_num),
CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) >9 THEN SPLIT(campaign_name,'_')[OFFSET(1)] ELSE 'Other' END AS campaign_descr,
CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) >9 THEN SPLIT(campaign_name,'_')[OFFSET(9)] ELSE 'Other' END AS audience_name,
CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) >9 THEN SPLIT(campaign_name,'_')[OFFSET(2)] ELSE 'Other' END AS ad_format,
'Other' as ad_format_detail,
'Outbrain' as publisher,
'Outbrain' as platform,
'Other' as creative_descr,
'Other' as media_format,
ad_name as creative_name
FROM deduplicate_data LEFT JOIN ad ON deduplicate_data.ad_id = ad.ad_id