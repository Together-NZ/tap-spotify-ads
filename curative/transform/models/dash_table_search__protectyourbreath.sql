{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT * FROM `curative-main.google_ads_protectyourbreath_search_transformed.google_ads_search__protectyourbreath`
)
SELECT *,
CASE WHEN 
 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       else 'Other'
END as media_format,
'Paid Search' as channel,
CASE WHEN 'INTENT' IN (select distinct funnel from `curative-main.dash_table__protectyourbreath.dash_table__protectyourbreath`) then 'INTENT'
ELSE 'OTHER' END as funnel, 
NULL AS creative_name,
NULL AS ad_format, 
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


