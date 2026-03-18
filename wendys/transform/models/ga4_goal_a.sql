{{ config(
    materialized='table',
) }}

{{ ga4.ga4_goal_a(source_name='ga4_raw', table_name='goal', plan_code='wen') }}