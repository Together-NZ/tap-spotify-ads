{{ config(
    materialized='table',
) }}
{{ga4.ga4_goal_ecommerce(source_name='ga4_raw', table_name='ecommerce_goal', dash_source_name='dash_union', dash_table_name='dash_union') }}