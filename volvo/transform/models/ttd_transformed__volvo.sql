{{ config(
    materialized='table',
) }}
{{ ttd.ttd(source_name='ttd_raw__volvo', table_name='standard_streams') }}