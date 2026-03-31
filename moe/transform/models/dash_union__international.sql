{{
    config(
        materialized='table',
    )
}}
WITH funnel_old AS (
SELECT *  FROM `moe-main.dash_table.dash_union` WHERE campaign_name NOT IN (
    SELECT DISTINCT campaign_name FROM (
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
))),
new_funnel_paid_media AS (
SELECT * EXCEPT(funnel), 
CASE WHEN 
EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE LOWER(a) LIKE '%canada%') THEN 'Canada'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE LOWER(a) LIKE '%us%' OR LOWER(a) LIKE '%usa%' OR LOWER(a) LIKE '%united states%' OR LOWER(a) LIKE '%america%') THEN 'USA'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE LOWER(a) LIKE '%germany%' OR LOWER(a) LIKE '%german%' OR LOWER(a) LIKE '%german%') THEN 'Germany'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE LOWER(a) LIKE '%finland%' OR LOWER(a) LIKE '%fi%' OR LOWER(a) LIKE '%finnish%') THEN 'Finland'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE LOWER(a) LIKE '%norway%' OR LOWER(a) LIKE '%no%' OR LOWER(a) LIKE '%norwegian%') THEN 'Norway'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE LOWER(a) LIKE '%denmark%' OR LOWER(a) LIKE '%dk%' OR LOWER(a) LIKE '%danish%') THEN 'Denmark'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE LOWER(a) LIKE '%netherlands%' OR LOWER(a) LIKE '%nl%' OR LOWER(a) LIKE '%dutch%') THEN 'Netherlands'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE (LOWER(a) like '%uk%' OR LOWER(a) like '%united kingdom%' OR LOWER(a) like '%british%') AND NOT
(LOWER(a) LIKE '%ireland%' OR LOWER(a) LIKE '%irish%' OR LOWER(a) LIKE '%ir%')) THEN 'UK'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE (LOWER(a) LIKE '%ireland%' OR LOWER(a) LIKE '%irish%' OR LOWER(a) LIKE '%ir%') AND 
    NOT (LOWER(a) LIKE '%uk%' OR LOWER(a) LIKE '%united kingdom%' OR LOWER(a) LIKE '%british%')) THEN 'Ireland'
WHEN EXISTS (SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) AS a 
WHERE (LOWER(a) like '%uk%' OR LOWER(a) like '%united kingdom%' OR LOWER(a) like '%british%') AND (
    LOWER(a) LIKE '%ireland%' OR LOWER(a) LIKE '%irish%' OR LOWER(a) LIKE '%ir%') )THEN 'UK & Ireland'
ELSE 'Other'
END AS funnel
FROM funnel_old WHERE NOT ((publisher) = 'Search' OR (publisher) = 'Performance Max')
)
(
    SELECT 
    * EXCEPT(funnel),funnel
    FROM funnel_old WHERE (publisher) = 'Search' OR (publisher) = 'Performance Max'
) UNION ALL 
(
    SELECT * FROM new_funnel_paid_media
)