{{
    config(
        materialized='table',
    )
}}
SELECT * FROM `moe-main.dash_table.dash_union` WHERE (LOWER(campaign_name) LIKE '%nz%' OR LOWER(campaign_name) LIKE '%domestic%'
or LOWER(campaign_name) LIKE '%new zealand%')