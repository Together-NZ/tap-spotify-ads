{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_a(source_name='ga4_raw__great_journey', table_name='goal',plan_code=env_var('PLAN_CODE_GREAT_JOURNEY', 'gjnz')) }}