{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack('hivestack_raw', 'report') }}