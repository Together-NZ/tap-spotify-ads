{{ config(
    materialized='table',
) }}
with basic as (
SELECT 
  JSON_VALUE(data, '$.datetime') AS date,
  CAST(JSON_VALUE(data, '$.campaign_id') AS INT64) AS campaign_id,
  JSON_VALUE(data, '$.city') AS city,
  JSON_VALUE(data, '$.creative') AS creative_name,
  JSON_VALUE(data, '$.line_item') AS line_item,
  CAST(JSON_VALUE(data, '$.line_item_id') AS INT64) AS line_item_id,
  CAST(REPLACE(JSON_VALUE(data, '$.concentration'), '%', '') AS FLOAT64) AS concentration,
  CAST(JSON_VALUE(data, '$.impressions') AS FLOAT64) AS impressions,
  CAST(JSON_VALUE(data, '$.plays') AS INT64) AS plays,
  CAST(JSON_VALUE(data, '$.progress') AS FLOAT64) AS progress,
  CAST(JSON_VALUE(data, '$.spend') AS FLOAT64) AS media_cost,
  ROW_NUMBER() OVER (
      PARTITION BY CAST(JSON_VALUE(data, '$.campaign_id') AS INT64), CAST(JSON_VALUE(data, '$.line_item_id') AS INT64),JSON_VALUE(data, '$.datetime'),
      JSON_VALUE(data, '$.creative'),CAST(JSON_VALUE(data, '$.plays') AS INT64)
  ) as row_num ,
  FROM `wendys-main.hivestack_raw.wendys_report`),
  campaign_data AS (
    SELECT DISTINCT JSON_VALUE(data,'$.campaign') AS campaign_name,json_value(data,'$.campaign_id') AS campaign_id,row_number() over (
      partition by json_value(data,'$.campaign_id') order by json_value(data,'$.datetime') desc
    ) as row_number FROM `wendys-main.hivestack_raw.wendys_report`
    
  ),
  distinct_campaign_name AS (
    SELECT campaign_name ,campaign_id from campaign_data where row_number = 1
  ),
  deduplicate_data AS (
    SELECT date,campaign_id,city,creative_name,line_item,line_item_id,concentration,impressions,plays,progress,media_cost,
    row_num FROM basic where row_num = 1
  ) , 
  joint_data AS (
    SELECT deduplicate_data.*, distinct_campaign_name.campaign_name as campaign_name
    FROM deduplicate_data join distinct_campaign_name on SAFE_CAST(deduplicate_data.campaign_id AS STRING) = distinct_campaign_name.campaign_id
  ),
  
  populate_data as (
  SELECT SUM(impressions) AS impressions,
  SUM(media_cost) AS media_cost,
  SUM(plays) AS plays,
  campaign_name,
  campaign_id,
  city,
  creative_name,
  line_item,
  CASE 
    WHEN ARRAY_LENGTH(SPLIT(line_item,'_'))>=8 THEN SPLIT(line_item,'_')[7]
    ELSE 'Other'
  END AS audience_name,
  CASE
    WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>1 THEN SPLIT(campaign_name,'_')[1]
    ELSE 'Other'
  END AS campaign_descr,
  CASE 
    WHEN ARRAY_LENGTH(SPLIT(creative_name,'_')) <8 THEN 'Other'
    ELSE
        SPLIT(creative_name,'_')[OFFSET(7)] 
  END AS ad_format_detail,
  CASE 
    WHEN ARRAY_LENGTH(SPLIT(creative_name,'_')) <8 THEN 'Other'
    ELSE
        SPLIT(creative_name,'_')[OFFSET(5)] 
  END AS creative_descr,
  CASE 
    WHEN ARRAY_LENGTH(SPLIT(creative_name,'_')) <8 THEN 'Other'
    ELSE
        SPLIT(creative_name,'_')[OFFSET(6)] 
  END AS ad_format,
  'Hivestack' AS publisher,
  'PDOOH' AS media_format,
  line_item_id,
  concentration,
  date,
  progress
  FROM joint_data
  GROUP BY campaign_name,campaign_id,city,creative_name,line_item,line_item_id,concentration,date,progress

 
  )
SELECT SUM(impressions) as impressions,SUM(media_cost) as media_cost, ad_format,campaign_name, creative_name,0 as clicks ,audience_name,ad_format_detail,DATE(date) as date ,publisher, campaign_descr,creative_descr FROM populate_data group by campaign_name,creative_name,ad_format_detail,campaign_descr,audience_name,publisher,creative_descr,date,ad_format