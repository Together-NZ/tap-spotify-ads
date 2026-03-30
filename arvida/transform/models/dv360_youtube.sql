-- models/get_yt_ads_data_deduplicated.sql
{{ config(
    materialized='table',
) }}
{{ dv360.dv360_youtube(source_name='dv360_raw', table_name='dv360_youtube', plan_code=env_var('PLAN_CODE', 'arv'), dv360_standard_name='dv360_standard') }}