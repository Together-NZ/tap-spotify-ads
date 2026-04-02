{{ config(
    materialized='table',
) }}
{{ttd.ttd(source_name='ttd_raw', table_name='standard_streams')}}
