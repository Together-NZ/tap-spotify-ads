{{ config(
    materialized='table',
) }}

with dash_table AS (
    {{ dash_table_general_process.google_ads(source_name='google_ads', table_name='google_ads_demand') }}
    UNION ALL
    {{ dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook') }}
    UNION ALL
    {{ dash_table_general_process.ttd(source_name='ttd_transformed', table_name='ttd') }}
    UNION ALL
    {{ dash_table_general_process.linkedin(source_name='linkedin_transformed', table_name='linkedin') }}
),
with_channel AS (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)

),
{{dash_table_general_process.dash_table_general_process()}}