{{ config(
    materialized='table',
) }}
SELECT * DISTINCT campaign_name FROM 