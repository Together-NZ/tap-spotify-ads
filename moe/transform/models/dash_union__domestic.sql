{{
    config(
        materialized='table',
    )
}}
SELECT * FROM `moe-main.dash_table.dash_union` WHERE ( LOWER(campaign_name) LIKE '%domestic%')
UNION ALL
SELECT * FROM `moe-main.dash_table.dash_union` WHERE LOWER(campaign_name) LIKE '%hivestack%'
UNION ALL
SELECT * FROM `moe-main.dash_table.dash_union` WHERE ((LOWER(campaign_name) LIKE '%nz%' OR LOWER(campaign_name) LIKE '%new zealand%') 
AND (LOWER(campaign_name) NOT LIKE '%international%' 
AND LOWER(campaign_name) NOT LIKE '%oversea%'))
UNION ALL 
SELECT * FROM `moe-main.dash_table.dash_union` WHERE (LOWER(campaign_name) NOT LIKE '%international%' 
AND LOWER(campaign_name) NOT LIKE '%oversea%' AND LOWER(campaign_name) NOT LIKE '%hivestack%' 
AND LOWER(campaign_name) NOT LIKE '%domestic%'
AND LOWER(campaign_name) NOT LIKE '%nz%'
AND LOWER(campaign_name) NOT LIKE '%new zealand%')