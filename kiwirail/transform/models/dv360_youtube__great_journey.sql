{{ config(
    materialized='table',
) }}
{{dv360.dv360_youtube(source_name='dv360_raw__great_journey', table_name='dv360_youtube',plan_code=env_var('PLAN_CODE_GREAT_JOURNEY', 'gjnz'))}}