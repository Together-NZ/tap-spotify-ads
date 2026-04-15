{{ config(
    materialized='table',
) }}
{{dv360.dv360_standard(source_name='dv360_raw', table_name='dv360_standard')}}
