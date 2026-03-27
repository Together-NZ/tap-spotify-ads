{{ config(
    materialized='table',
) }}
SELECT * FROM `moe-main.ga4_transformed.ga4_goal_channel` WHERE (
    campaign_name IN (
        SELECT DISTINCT campaign_name 
        FROM `moe-main.dash_table__international.dash_union__international`
    ) OR (campaign_name NOT LIKE '%moe%' AND country != 'New Zealand')
)