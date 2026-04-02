{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack(table_name='hivestack_raw__lotus', report_name=env_var('REPORT_NAME', 'lotus_report')) }}