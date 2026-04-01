{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack('hivestack_raw',env_var('REPORT_NAME', 'barfoot_report')) }}