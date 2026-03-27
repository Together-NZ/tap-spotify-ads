{{
    config(
        materialized='table',
    )
}}
SELECT * FROM `moe-main.dash_table.dash_union` WHERE NOT (LOWER(campaign_name) LIKE '%nz%' OR LOWER(campaign_name) LIKE '%domestic%'
OR LOWER(campaign_name) LIKE '%new zealand%')