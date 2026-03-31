{{ config(
    materialized='table',
) }}
WITH ga4_funnel_old AS (
SELECT * EXCEPT(funnel)


FROM `moe-main.ga4_transformed.ga4_goal_channel` WHERE (
    campaign_name IN (
        SELECT DISTINCT campaign_name 
        FROM `moe-main.dash_table__international.dash_union__international`
    ) OR (lower(campaign_name) NOT LIKE '%moe%' AND country != 'New Zealand')
)
),
campaign_name_funnel AS (
    SELECT DISTINCT campaign_name,funnel FROM `moe-main.dash_table__international.dash_union__international`
)
SELECT ga4.*,
CASE WHEN funnel IS NULL THEN 'OTHER' ELSE funnel END AS funnel
FROM ga4_funnel_old AS ga4 LEFT JOIN 
campaign_name_funnel AS cf ON LOWER(ga4.campaign_name) = LOWER(cf.campaign_name)