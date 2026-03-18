{{ config(
    materialized='table',
) }}
with {{reddit.reddit_ads(source_name='reddit_raw', table_name='ads')}},
{{reddit.reddit_campaigns(source_name='reddit_raw', table_name='campaigns')}},
{{reddit.reddit_reports(source_name='reddit_raw', table_name='reports')}},
{{reddit.reddit_final()}}