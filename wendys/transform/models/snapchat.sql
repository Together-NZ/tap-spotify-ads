{{ config(
    materialized='table',
) }}

WITH 
{{ snapchat.ad_stats_daily(source_name='snapchat_raw', table_name='ad_stats_daily') }},
{{snapchat.ad_detail(source_name='snapchat_raw', table_name='ads') }},
{{snapchat.ad_squad(source_name='snapchat_raw', table_name='ad_squads') }},
{{snapchat.campaigns(source_name='snapchat_raw', table_name='campaigns') }},
{{snapchat.final_calculation()}}