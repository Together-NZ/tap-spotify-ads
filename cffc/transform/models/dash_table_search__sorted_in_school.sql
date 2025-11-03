{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT * FROM `cffc-main.google_ads_search_transformed__sorted_in_school.google_ads_search__sorted_in_school`
)
SELECT *
EXCEPT(row_num),
CASE WHEN 
 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       else 'Other'
END as media_format,
CASE WHEN lower(publisher) = 'demand gen' THEN 'Demand Gen'
ELSE 'Paid Search' END as channel,
NULL AS creative_name,
NULL AS ad_format, 
NULL AS ad_format_detail,
NULL AS audience_name,
NULL AS video_completion,
NULL AS video_50_completion,
NULL AS video_25_completion,
NULL AS video_75_completion,
null as video_views,
CASE WHEN 'INTENT' IN (select distinct funnel from `cffc-main.dash_table__cffc.dash_table__cffc`) then 'INTENT'
ELSE 'OTHER' END as funnel,
null as campaign_descr,
null as creative_descr
from dash_table