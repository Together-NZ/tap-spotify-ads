{{ config(
    materialized='table',
) }}
SELECT * FROM `moe-main.ga4_transformed.ga4_goal_channel` WHERE country = 'New Zealand'