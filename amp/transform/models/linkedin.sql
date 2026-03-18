{{ config(
    materialized='table',
) }}
WITH {{linkedin.lk_ad_analytics_by_creative(source_name='linkedin_raw', table_name='ad_analytics_by_creative')}},
{{linkedin.lk_creative_campaign_link(source_name='linkedin_raw', table_name='creatives')}},
{{linkedin.lk_campaign_group_campaign_link(source_name='linkedin_raw', table_name='campaign_groups')}},
{{linkedin.lk_campaign_name_joining_update(source_name='linkedin_raw', table_name='campaigns')}}
{{linkedin.lk_final_output()}}