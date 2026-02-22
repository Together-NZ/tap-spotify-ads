{{ config(
    materialized='table',
) }}
SELECT * FROM `real-nz-main.tiktok_transformed__mountain.tiktok__mountain` 
WHERE LOWER(campaign_name) like '%rlnz%'