{{ config(
    materialized='table',
) }}
{{ ga4.ga4_goal_a(source_name='ga4_raw__interislander', table_name='goal',dash_union_source_name='dash_union__interislander',dash_union_table_name='dash_union__interislander') }}
