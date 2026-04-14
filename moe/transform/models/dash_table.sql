{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    {{ dash_table_general_process.ttd(source_name='ttd_transformed', table_name='ttd_transformed') }}

    UNION ALL
    {{ dash_table_general_process.dv360_standard(source_name='dv360_transformed', table_name='dv360_standard',yt_source_name='dv360_transformed',yt_table_name='dv360_youtube') }}
    UNION ALL
    {{ dash_table_general_process.dv360_youtube(source_name='dv360_transformed', table_name='dv360_youtube') }}
    UNION ALL
    {{ dash_table_general_process.tiktok(source_name='tiktok_transformed', table_name='tiktok') }}
    UNION ALL
    {{ dash_table_general_process.meta(source_name='facebook_transformed', table_name='facebook') }}
    UNION ALL
    {{ dash_table_general_process.snapchat(source_name='snapchat_transformed', table_name='snapchat') }}
    UNION ALL
    {{ dash_table_general_process.linkedin(source_name='linkedin_transformed', table_name='linkedin') }}
    UNION ALL
    {{ dash_table_general_process.cm360(source_name='cm360_transformed', table_name='cm360_direct_buy') }}
UNION ALL
{{ dash_table_general_process.reddit(source_name='reddit_transformed', table_name='reddit') }}
UNION ALL 
{{ dash_table_general_process.hivestack(source_name='hivestack_transformed', table_name='hivestack') }}
),
with_channel AS (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel


FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(dt.publisher) = lower(dc.publisher)),
{{dash_table_general_process.dash_table_general_process()}}