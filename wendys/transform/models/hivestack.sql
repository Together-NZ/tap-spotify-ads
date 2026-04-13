{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack(table_name='hivestack_raw', report_name=env_var('REPORT_NAME', 'wendys_report')) }}