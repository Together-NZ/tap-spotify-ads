{{ config(
    materialized='table',
) }}
{{ ttd.ttd(source_name='ttd_raw__geely', table_name='standard_streams') }}