{{ config(
    materialized='table',
) }}
WITH {{facebook.daily_breakdown(source_name='facebook_raw__great_journey', table_name='ads_insights')}},
{{facebook.conversion_goal(source_name='facebook_raw__great_journey', table_name='ad_sets')}},
{{facebook.custom_conversions(source_name='facebook_raw__great_journey', table_name='custom_conversions')}},
{{facebook.joining(source_name='facebook_raw__great_journey', table_name='ads_insights')}},
{{facebook.joining_adset_to_ads_tracking(source_name='facebook_raw__great_journey', table_name='ads')}},
{{facebook.metric_calculation()}},
{{facebook.ad_data(source_name='facebook_raw__great_journey', table_name='ads')}},
{{facebook.adset_data(source_name='facebook_raw__great_journey', table_name='ad_sets')}},
{{facebook.campaign_data(source_name='facebook_raw__great_journey', table_name='campaigns')}},
{{facebook.interest_data(source_name='facebook_raw__great_journey', table_name='ads')}},
{{facebook.final_calculation()}}