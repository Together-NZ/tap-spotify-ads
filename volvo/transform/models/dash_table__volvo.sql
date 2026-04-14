{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{dash_table_general_process.ttd(source_name='ttd_transformed', table_name='ttd_transformed__volvo') }}

    UNION ALL
    {{dash_table_general_process.dv360_youtube(source_name='dv360_transformed', table_name='dv360_youtube__volvo') }}
    UNION ALL
    {{dash_table_general_process.linkedin(source_name='linkedin_transformed', table_name='linkedin__volvo') }}
    UNION ALL

     {{ dash_table_general_process.dv360_standard(source_name='dv360_transformed', table_name='dv360_standard__volvo',yt_source_name='dv360_transformed',yt_table_name='dv360_youtube__volvo') }}
    UNION ALL
    {{dash_table_general_process.google_ads(source_name='google_ads', table_name='google_ads_demand__volvo') }}
    
    UNION ALL
    {{dash_table_general_process.hivestack(source_name='hivestack_transformed', table_name='hivestack__volvo') }}
    UNION ALL
    {{dash_table_general_process.google_ads(source_name='google_ads', table_name='google_ads_demand_2__volvo') }}
    UNION ALL
    {{dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook__volvo') }}

    UNION ALL
    {{dash_table_general_process.cm360(source_name='cm360_transformed', table_name='cm360_direct_buy__volvo') }}
),
with_channel AS (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)

),
{{dash_table_general_process.dash_table_general_process()}}