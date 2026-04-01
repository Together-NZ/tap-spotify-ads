{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel(source_name='dash_union', table_name='dash_union', plan_code=env_var('PLAN_CODE_GA4', 'pol'), ga4_goal_a_model='ga4_goal_a') }}