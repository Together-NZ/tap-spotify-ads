{{ config(
    materialized='table',
) }}
--IF EXISTS (SELECT * FROM `arvida-main.google_ads_good_friends_search_transformed.google_ads_good_friends_search`)
with dash_table AS ((SELECT * 
    from `aia-nz-main.google_ads_search_transformed__brand.google_ads_search_brand`
) UNION ALL (
    SELECT * 
    from `aia-nz-main.google_ads_search_transformed__marketing.google_ads_search_marketing`
) ),
funnels as (
    select distinct funnel from `aia-nz-main.dash_table.dash_table`
)
SELECT *
,
CASE WHEN 
 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       else 'Other'
END as media_format,
'Paid Search' as channel,
NULL AS creative_name,
NULL AS ad_format, 
CASE WHEN 'INTENT' IN (select distinct funnel from `aia-nz-main.dash_table.dash_table`) then 'INTENT'
ELSE 'Other' END as funnel,
NULL AS ad_format_detail,
NULL AS audience_name,
NULL AS video_completion,
NULL AS video_50_completion,
NULL AS video_25_completion,
NULL AS video_75_completion,
null as video_views,
null as campaign_descr,
null as creative_descr
from dash_table