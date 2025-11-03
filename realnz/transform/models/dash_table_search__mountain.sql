{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT * FROM `real-nz-main.google_ads_search_transformed__mountain.google_ads_search__mountain`
)
SELECT *,
CASE WHEN LOWER(campaign_name) like '%search%' or (publisher) = 'Search' THEN 'SEARCH'
WHEN LOWER(campaign_name) like '%pmax%' or lower(campaign_name) like '%performance max%' OR LOWER(campaign_name) like '%performance-max%' THEN 'PERFORMANCE MAX'
WHEN LOWER(campaign_name) like '%google%'  AND ( lower(campaign_name) like '%native%' OR LOWER(campaign_name) like '%demand gen%')THEN 'DEMAND GEN'
ELSE 'OTHER'
END as media_format,
CASE WHEN lower(publisher) = 'demand gen' THEN 'Demand Gen'
ELSE 'Paid Search' END as channel,
NULL AS creative_name,
NULL AS ad_format, 
CASE WHEN 'BOOK' IN (select distinct funnel from `real-nz-main.dash_table__mountain.dash_table__mountain`) and lower(publisher) != 'demand gen' then 'BOOK'
WHEN lower(publisher) = 'demand gen' AND 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) as a
       WHERE LOWER(a) IN UNNEST(ARRAY['explore','compare','book','dream']))
       THEN (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE LOWER(X) IN UNNEST(['explore','compare','book','dream'])
       LIMIT 1)
ELSE 'OTHER'
END AS funnel,
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

