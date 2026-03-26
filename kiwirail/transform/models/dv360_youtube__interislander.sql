{{ config(
    materialized='table',
) }}
{{dv360.dv360_youtube(source_name='dv360_raw__interislander', table_name='dv360_youtube',plan_code=env_var('PLAN_CODE_INTERISLANDER', 'iil'))}}