{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack(table_name='hivestack_raw__volvo', report_name=env_var('REPORT_NAME', 'volvo_report')) }}