{{ config(
    materialized='table',
) }}



WITH dash_table AS (
       {{dash_table_general_process.ttd(source_name='ttd_transformed', table_name='ttd_transformed') }}

    UNION ALL
    {{dash_table_general_process.dv360_standard(source_name='dv360_transformed', table_name='dv360_standard') }}
    
    UNION ALL
    {{dash_table_general_process.dv360_youtube(source_name='dv360_transformed', table_name='dv360_youtube') }}
    UNION ALL
    {{dash_table_general_process.google_ads(source_name='google_ads', table_name='google_ads_demand') }}
    UNION ALL
    {{dash_table_general_process.tiktok(source_name='tiktok_transformed', table_name='tiktok') }}

    UNION ALL
    {{dash_table_general_process.snapchat(source_name='snapchat_transformed', table_name='snapchat') }}
    UNION ALL

    {{dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook') }}
    UNION ALL
    {{dash_table_general_process.cm360(source_name='cm360_transformed', table_name='cm360_direct_buy') }}
    UNION ALL 
    {{dash_table_general_process.hivestack(source_name='hivestack_transformed', table_name='hivestack') }}

   
),
with_channel as (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(trim(dt.publisher)) = lower(trim(dc.publisher))),
{{dash_table_general_process.dash_table_general_process()}}