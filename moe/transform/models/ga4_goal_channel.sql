{{ config(
    materialized='table',
) }}
SELECT * FROM `moe-main.ga4_transformed__career.ga4_goal_channel__career`
UNION ALL 
SELECT * FROM `moe-main.ga4_transformed__education.ga4_goal_channel__education`