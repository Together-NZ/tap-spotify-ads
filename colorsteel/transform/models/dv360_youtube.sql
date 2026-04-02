{{ config(
    materialized='table',
) }}
{{ dv360.dv360_youtube(source_name='dv360_raw', table_name='dv360_youtube',dv360_standard_name='dv360_standard') }}
