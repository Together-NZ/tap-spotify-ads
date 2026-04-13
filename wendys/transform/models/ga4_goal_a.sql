{{ config(
    materialized='table',
) }}

{{ ga4.ga4_goal_a(source_name='ga4_raw', table_name='goal',dash_union_source_name='dash_union',dash_union_table_name='dash_union') }}