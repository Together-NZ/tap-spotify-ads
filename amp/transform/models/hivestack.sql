{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack('hivestack_raw','amp_report') }}