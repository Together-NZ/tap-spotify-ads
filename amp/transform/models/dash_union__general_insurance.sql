{{ config(
    materialized='table',
) }}
SELECT * FROM `amp-main.dash_table.dash_union__centralized`WHERE sub_brands='General Insurance'