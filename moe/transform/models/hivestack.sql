{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack(table_name='hivestack_raw', report_name='moe_report') }}