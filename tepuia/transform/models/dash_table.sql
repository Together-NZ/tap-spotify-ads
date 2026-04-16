{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.ttd(source_name='ttd_transformed', table_name='ttd_transformed') }}

    UNION ALL

    {{ dash_table_general_process.dv360_standard(source_name='dv360_transformed', table_name='dv360_standard', yt_source_name='dv360_transformed', yt_table_name='dv360_youtube') }}

    UNION ALL

    {{ dash_table_general_process.dv360_youtube(source_name='dv360_transformed', table_name='dv360_youtube') }}

    UNION ALL

    {{ dash_table_general_process.google_ads(source_name='google_ads', table_name='google_ads_demand_2') }}

    UNION ALL

    {{ dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook') }}

    UNION ALL

    {{ dash_table_general_process.google_ads(source_name='google_ads', table_name='google_ads_demand') }}

    UNION ALL

    {{ dash_table_general_process.cm360(source_name='cm360_transformed', table_name='cm360_direct_buy') }}
),
with_channel AS (
SELECT * EXCEPT (publisher, channel),
dc.publisher,
dc.channel

FROM dash_table AS dt
JOIN `together-internal.channel.publisher_channel` AS dc
  ON lower(trim(dt.publisher)) = lower(trim(dc.publisher))

),
{{dash_table_general_process.dash_table_general_process()}}
