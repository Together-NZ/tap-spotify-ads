{{ config(
    materialized='table',
) }}
select * from `amp-main.adobe_transformed.adobe` where sub_brands='direct'