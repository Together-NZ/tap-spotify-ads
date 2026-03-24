{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_channel(source_name='dash_union__great_journey', table_name='dash_union__great_journey', plan_code=env_var('PLAN_CODE_GREAT_JOURNEY', 'gjnz'), ga4_goal_a_model='ga4_goal_a__great_journey') }}