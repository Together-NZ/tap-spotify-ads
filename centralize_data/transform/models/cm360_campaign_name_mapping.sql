  {{ config(
    materialized='table',
) }}
WITH base AS (
    SELECT distinct JSON_VALUE(data,'$.campaign_name'),campaign_id,max(date) from `together-internal.cm360_raw.cm360_report_stream`
)