{{ config(
    materialized='table',
) }}
{{ ttd.ttd(source_name='ttd_raw__lotus', table_name='standard_streams') }}