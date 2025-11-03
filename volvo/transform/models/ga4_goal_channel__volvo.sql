{{ config(
    materialized='table',
) }}
WITH channels as (
  SELECT *,
    -- Define channel using the resolved site_name field in this CTE
    CASE 
        WHEN LOWER(site_name) LIKE '%acast%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%organic_search' THEN 'Organic Search'
        WHEN LOWER(site_name) LIKE '%adfenix%' THEN 'Owned Display'
        WHEN LOWER(site_name) LIKE '%adobe%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%apple_ads_search%' THEN 'Paid Search'
        WHEN LOWER(site_name) LIKE '%bing_ads_search%' THEN 'Paid Search'
        WHEN LOWER(site_name) LIKE '%blis%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%business_desk%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%clearscore%' THEN 'Affiliate'
        WHEN LOWER(site_name) LIKE '%dart_search%' THEN 'Paid Search'
        WHEN LOWER(site_name) LIKE '%demand_gen%' THEN 'Demand Gen'
        WHEN LOWER(site_name) LIKE '%direct%' THEN 'Direct'
        WHEN LOWER(site_name) LIKE '%dv_360%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%dv360%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%email%' THEN 'Email'
        WHEN LOWER(site_name) LIKE '%meta%' and (lower(sessionCampaignName) like '%wat%' or lower(split(sessionCampaignName,'_')[safe_offset(0)]) like '%00%') THEN 'Paid Social'
        WHEN LOWER(site_name) LIKE '%gdn%' THEN 'Owned Display'
        WHEN LOWER(site_name) LIKE '%gmb%' THEN 'Google My Business'
        WHEN LOWER(site_name) LIKE '%google_ads_display%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%google_ads_search%' THEN 'Paid Search'
        WHEN LOWER(site_name) LIKE '%google_my_business%' THEN 'Google My Business'
        WHEN LOWER(site_name) LIKE '%homes.co.nz%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%linkedin%' THEN 'Paid Social'
        WHEN LOWER(site_name) LIKE '%listinglogic%' THEN 'Owned Display'
        WHEN LOWER(site_name) LIKE '%mediaworks%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%metservice%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%nzh_business%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%nzherald%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%nzme%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%organic_search%' THEN 'Organic Search'
        WHEN LOWER(site_name) LIKE '%organic_social%' THEN 'Organic Social'
        WHEN LOWER(site_name) LIKE '%outbrain%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%own_social%' or ((lower(sessionCampaignName) not like '%wat%' or lower(split(sessionCampaignName,'_')[safe_offset(0)]) not like '%00%') and lower(site_name)='meta') THEN 'Owned Social'
        WHEN LOWER(site_name) LIKE '%pinterest%' THEN 'Paid Social'
        WHEN LOWER(site_name) LIKE '%quora%' THEN 'Paid Social'
        WHEN LOWER(site_name) LIKE '%referral%' THEN 'Referral'
        WHEN LOWER(site_name) LIKE '%rem%' THEN 'Owned Display'
        WHEN LOWER(site_name) LIKE '%snapchat%' THEN 'Paid Social'
        WHEN LOWER(site_name) LIKE '%spotify%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%stuff%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%teads%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%tiktok%' THEN 'Paid Social'
        WHEN LOWER(site_name) LIKE '%trademe%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%ttd%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%tvnz%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%twitch%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%verizon%' THEN 'Paid Display'
        WHEN LOWER(site_name) LIKE '%youtube%' THEN 'Paid Display'
        ELSE 'Unknown'
    END AS channel
  FROM {{ ref('ga4_goal_a__volvo') }}
),with_channel AS (
select *,
  CASE 
                WHEN LOWER(site_name) like '%own_social%' THEN 'Owned Social'
                WHEN LOWER(site_name) LIKE '%meta%' AND (lower(sessionCampaignName) like '%wat%' or lower(split(sessionCampaignName,'_')[safe_offset(0)]) like '%00%') and channel like '%Paid%' THEN 'Meta'
                WHEN LOWER(site_name) LIKE '%demand_gen%' THEN 'Demand Gen'
                WHEN LOWER(site_name) LIKE '%organic_search' THEN 'Organic Search'
                WHEN LOWER(sessionSourceMedium) LIKE '%ttd%' THEN 'Ttd'
                WHEN LOWER(site_name) LIKE '%email%' THEN 'Email'
                WHEN LOWER(site_name) LIKE '%tiktok%' AND (lower(sessionCampaignName) like '%wat%' or lower(split(sessionCampaignName,'_')[safe_offset(0)]) like '%00%') and channel like '%Paid%' THEN 'Tiktok'
                WHEN LOWER(sessionSourceMedium) LIKE '%snapchat%' THEN 'Snapchat'
                WHEN LOWER(sessionSourceMedium) LIKE '%youtube%' OR LOWER(sessionSourceMedium) LIKE '%yt%' or lower(sessionCampaignName) like '%yt%' THEN 'Youtube'
                WHEN LOWER(sessionSourceMedium) LIKE '%3now%' OR LOWER(sessionSourceMedium) LIKE '%three%' THEN 'Threenow'
                WHEN LOWER(sessionSourceMedium) LIKE '%nzme%' THEN 'Nzme'
                WHEN LOWER(sessionSourceMedium) LIKE '%acast%' THEN 'Acast'
                WHEN (lower(site_name) like '%referral%') THEN 'Referral'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) and (lower(sessionCampaignName) like '%youtube%'
                or lower(sessionCampaignName) like '%yt%' or lower(sessionManualAdContent) like '%yt%' or lower(sessionManualAdContent) like '%youtube%') THEN 'Youtube'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) and (lower(sessionCampaignName) like '%nzme%' 
                or lower(sessionManualAdContent) like '%nzme%') THEN 'Nzme'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) and (lower(sessionCampaignName) like '%tvnz%' or lower(sessionManualAdContent)
                like '%tvnz%') THEN 'Tvnz'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) and (lower(sessionCampaignName) like '%3now%' or lower(sessionManualAdContent)
                like '%three%' or lower(sessionManualAdContent) like '%3now%' or lower(sessionCampaignName) like '%three%') THEN 'Threenow'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%') and (lower(sessionCampaignName) like '%stuff%' or lower(sessionManualAdContent) like '%stuff%' )
                THEN 'Stuff'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name)like '%metservice%') and (lower(sessionCampaignName) like '%metservice%' or lower(sessionManualAdContent) like '%metservice%') THEN 'Metservice'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name)like '%mediaworks%') and (lower(sessionCampaignName) like '%mediaworks%' or lower(sessionManualAdContent) like '%mediaworks%') THEN 'Mediaworks'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name) like '%nzme%') and (lower(sessionCampaignName) like '%buseinss%' and lower(sessionCampaignName) like 'desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name) like '%nzme%') and (lower(sessionManualAdContent) like '%buseinss%' and lower(sessionManualAdContent) like 'desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) like '%business%' and lower(site_name) like '%desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) AND (LOWER(sessionCampaignName) like '%acast%' or lower(sessionManualAdContent) like '%acast%') THEN 'Acast'
                WHEN (LOWER(sessionCampaignName) LIKE '%perf%' and lower(sessionCampaignName) like '%max%') or lower(sessionCampaignName) like '%pmax%' THEN 'Performance Max'
                WHEN LOWER(site_name) LIKE '%google_ads_search%' THEN 'Search'
                ELSE TRIM(INITCAP(sessionSourceMedium))
            END AS publisher,

from channels),
  campaign_base AS (
       SELECT *,
              CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=2 
              THEN SPLIT(campaign_name,'_')[1] 
              ELSE campaign_name
       END as campaign_name_raw
       FROM with_channel

),
campaign_name_selection_duplicate AS (
       SELECT COUNT(*) AS indicator,lower(campaign_name_raw) AS lower_campaign FROM (SELECT DISTINCT campaign_name_raw FROM campaign_base)
       GROUP BY LOWER(campaign_name_raw) HAVING COUNT(*)>1
),
duplicate_raw AS (
       SELECT distinct campaign_name_raw, ROW_NUMBER() OVER (PARTITION BY LOWER(campaign_name_raw) ORDER BY (campaign_name_raw)) as row_number from campaign_base cb join campaign_name_selection_duplicate cd
       ON LOWER(cb.campaign_name_raw) = LOWER(cd.lower_campaign) 
),
deduplicate_raw AS (
       select * from duplicate_raw where row_number = 1
),
final_result AS (
SELECT camb.* EXCEPT(campaign_name_raw),
CASE WHEN 
       lower(camb.campaign_name_raw) = lower(deduplicate_raw.campaign_name_raw) 
       
       THEN deduplicate_raw.campaign_name_raw
       ELSE camb.campaign_name_raw
END AS campaign_name_selection
 FROM campaign_base camb LEFT JOIN deduplicate_raw ON LOWER(deduplicate_raw.campaign_name_raw) = LOWER(camb.campaign_name_raw)
),
funnel_campaign AS (
     select distinct funnel, campaign_name from `volvo-main.dash_table__volvo.dash_union__volvo`
),
dash AS (
     SELECT DISTINCT 
          TRIM(creative_descr) AS old_creative,
          --campaign_name AS platform_campaigns,
          REGEXP_REPLACE(TRIM(creative_descr), r'[_\-\s]+', '') AS creative_name,
          ROW_NUMBER() OVER(PARTITION BY LOWER(creative_descr)) AS row_num
     FROM `volvo-main.dash_table__volvo.dash_union__volvo`
),
deduplicate_dash AS (
    SELECT old_creative,creative_name FROM dash WHERE row_num=1
),
semi_final AS (
SELECT
  -- Replace publisher from table2 if matched, else keep original
  COALESCE(t2.present, t1.publisher) AS publisher,
  t1.*
EXCEPT(publisher) -- exclude original publisher to avoid duplicate columns

FROM final_result AS t1
LEFT JOIN `together-internal.publisher_naming.publisher_naming` AS t2
  ON LOWER(trim(t1.publisher)) = LOWER(trim(t2.publisher))),
ga4_tmp AS (
    SELECT DISTINCT TRIM(sessionManualAdContent) as ga4_creative_name,
      --campaign_name AS ga4_campaigns,
      REGEXP_REPLACE(TRIM(sessionManualAdContent), r'[_\-\s]+', '') AS creative_name,
      ROW_NUMBER() OVER (PARTITION BY LOWER(sessionManualAdContent)) AS row_num
    FROM semi_final
),
deduplicate_ga4 AS (
    SELECT ga4_creative_name,creative_name
    FROM ga4_tmp
    WHERE row_num=1
),
creative_matching AS (
  SELECT DISTINCT 
    ga4.ga4_creative_name,
    --ga4_tmp.ga4_campaigns,
    dash.old_creative,
    ROW_NUMBER() OVER (PARTITION BY LOWER(ga4.ga4_creative_name)) AS row_num
    --dash.date
  FROM deduplicate_ga4 AS ga4
  LEFT JOIN deduplicate_dash AS dash
    ON LOWER(dash.creative_name) = LOWER(ga4.creative_name)
  WHERE dash.creative_name IS NOT NULL
),
creative_matching_result AS (
  SELECT *EXCEPT(row_num) FROM creative_matching WHERE row_num=1
),
non_media_format as (

select 
COALESCE(t2.new_campaign_name,t1.campaign_name) AS campaign_name,
t1.* except(campaign_name)
from semi_final as t1 left join `together-internal.google_ads_campaign_mapping.campaign_name_mapping` as t2
on lower(trim(t1.campaign_name)) = lower(trim(t2.old_campaign_name))
)

SELECT md.* EXCEPT(sessionManualAdContent),
CASE WHEN cmr.old_creative IS NOT NULL THEN cmr.old_creative ELSE md.sessionManualAdContent END AS sessionManualAdContent,
COALESCE(fc.funnel,'OTHER') as funnel,
CASE
  -- hard override for SOCIAL
  WHEN
    LOWER(COALESCE(sessionSourceMediumraw, '')) LIKE '%social%' OR
    LOWER(COALESCE(md.campaign_name,  '')) LIKE '%social%' OR
    LOWER(TRIM(COALESCE(publisher,      ''))) LIKE '%facebook%' OR
    LOWER(TRIM(COALESCE(publisher,      ''))) LIKE '%meta%' OR
    LOWER(TRIM(COALESCE(publisher,      ''))) IN ('tiktok','snapchat','linkedin','twitter','x','instagram','pinterest')
  THEN 'SOCIAL'

  -- everything else
  WHEN LOWER(COALESCE(md.campaign_name,'')) LIKE '%vod%'    OR
       LOWER(COALESCE(md.campaign_name,'')) LIKE '%vidod%'  OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%vod%'    OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%vidod%'
  THEN 'VIDOD'

  WHEN LOWER(COALESCE(md.campaign_name,'')) LIKE '%dooh%'   OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%dooh%'
  THEN 'PDOOH'

  WHEN LOWER(COALESCE(md.campaign_name,'')) LIKE '%yt%'      OR
       LOWER(COALESCE(md.campaign_name,'')) LIKE '%youtube%' OR
       LOWER(COALESCE(md.campaign_name,'')) LIKE '%you tube%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%yt%'       OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%youtube%'  OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%you tube%'
  THEN 'YT'

  WHEN LOWER(COALESCE(md.campaign_name,'')) LIKE '%native%' OR
       LOWER(COALESCE(md.campaign_name,'')) LIKE '%nat%'    OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%native%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%nat%'
  THEN 'NATIVE'

  WHEN LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%disp%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%image%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%showcase%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%post%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%link%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%interstitial%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%mobile%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%pushdown%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE 'homepage%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%banner%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%direct%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%leadgen%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%interscroller%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%big reveal%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%big_reveal%'
  THEN 'DISP'

  WHEN LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%performance max%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%pmax%' OR
       LOWER(COALESCE(md.campaign_name,''))   LIKE '%pmax%' OR
       LOWER(COALESCE(md.campaign_name,''))   LIKE '%performance max%'
  THEN 'PERFORMANCE MAX'

  WHEN LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%aud%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%audience%' OR
       LOWER(TRIM(COALESCE(publisher,''))) = 'spotify'
  THEN 'AUD'

  WHEN LOWER(COALESCE(md.campaign_name,''))          LIKE '%vid%' OR
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%vid%'
  THEN 'VID'

  WHEN LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%rmdisp%'
  THEN 'RMDISP'

  WHEN (LOWER(COALESCE(md.campaign_name,''))    LIKE '%search%' AND
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%cpc%' ) OR sessionCampaignName='RLNZ005 - Travel Feed - Compare - NZ'
  THEN 'SEARCH'
  ELSE 'OTHER'
END AS media_format,

FROM non_media_format as md left join funnel_campaign AS fc on fc.campaign_name = md.campaign_name
LEFT JOIN creative_matching_result AS cmr ON lower(trim(md.sessionManualAdContent)) = lower(trim(cmr.ga4_creative_name))