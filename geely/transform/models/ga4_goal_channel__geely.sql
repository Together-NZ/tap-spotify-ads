{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel(source_name='dash_union__geely', table_name='dash_union__geely', plan_code=env_var('PLAN_CODE_GA4', 'geely'), ga4_goal_a_model='ga4_goal_a__geely') }}