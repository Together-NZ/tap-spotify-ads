{{ config(
    materialized='table',
) }}
select * from `amp-main.adobe_transformed.adobe` where sub_brands='General Insurance' or sub_brands='direct'