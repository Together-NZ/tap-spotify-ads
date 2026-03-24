{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT * FROM `kiwirail-main.google_ads_search_transformed__great_journey.google_ads_search__great_journey`
)
SELECT *,
CASE WHEN 
 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       else 'OTHER'
END as media_format,
CASE WHEN lower(publisher) = 'demand gen' THEN 'Demand Gen'
ELSE 'Paid Search' END as channel,
NULL AS creative_name,
NULL AS ad_format, 
CASE WHEN  lower(publisher) != 'demand gen' then 'INTENT'
WHEN lower(publisher) = 'demand gen' AND 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) as a
       WHERE LOWER(a) IN UNNEST(ARRAY['consideration','awareness','intent']))
       THEN (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE LOWER(X) IN UNNEST(['consideration','awareness','intent'])
       LIMIT 1)
ELSE 'OTHER'
END AS funnel,
CASE 

WHEN LOWER(campaign_name) like '%gi%' or lower(campaign_name) like '%general insurance%' 
or lower(campaign_name) like '%car%' or lower(campaign_name) like '%home%' or lower(campaign_name) like '%content%'
THEN 'General Insurance'
ELSE 'Wealth'
END AS sub_brands,
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