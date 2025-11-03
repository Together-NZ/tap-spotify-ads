{{ config(
    materialized='table',
) }}
WITH channels as (
  SELECT * ,
    -- Define channel using the resolved site_name field in this CTE
    CASE 
        WHEN LOWER(site_name) LIKE '%acast%' THEN 'Paid Display'
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
        WHEN LOWER(site_name) LIKE '%facebook%' THEN 'Paid Social'
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
        WHEN LOWER(site_name) LIKE '%own_social%' THEN 'Owned Social'
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
  FROM {{ ref('ga4_goal_a')}} ),
  with_channel AS (
select *,
  CASE 
                WHEN LOWER(site_name) LIKE '%facebook%' THEN 'Facebook'
                WHEN LOWER(site_name) LIKE '%demand_gen%' THEN 'Demand Gen'
                WHEN LOWER(site_name) like '%email%' THEN 'Email'
                WHEN LOWER(sessionSourceMedium) LIKE '%ttd%' THEN 'Ttd'
                WHEN LOWER(sessionSourceMedium) LIKE '%tiktok%' THEN 'Tiktok'
                WHEN LOWER(sessionSourceMedium) LIKE '%snapchat%' THEN 'Snapchat'
                WHEN LOWER(sessionSourceMedium) LIKE '%youtube%' OR LOWER(sessionSourceMedium) LIKE '%yt%' THEN 'Youtube'
                WHEN LOWER(sessionSourceMedium) LIKE '%3now%' OR LOWER(sessionSourceMedium) LIKE '%three%' THEN 'Threenow'
                WHEN LOWER(sessionSourceMedium) LIKE '%nzme%' THEN 'Nzme'
                WHEN LOWER(sessionSourceMedium) LIKE '%acast%' THEN 'Acast'
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
                WHEN (lower(site_name) like '%referral%') THEN 'Referral'
                WHEN (lower(site_name) like '%organic_search%') THEN 'Organic Search'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name)like '%metservice%') and (lower(sessionCampaignName) like '%metservice%' or lower(sessionManualAdContent) like '%metservice%') THEN 'Metservice'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name)like '%mediaworks%') and (lower(sessionCampaignName) like '%mediaworks%' or lower(sessionManualAdContent) like '%mediaworks%') THEN 'Mediaworks'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name) like '%nzme%') and (lower(sessionCampaignName) like '%buseinss%' and lower(sessionCampaignName) like 'desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name) like '%nzme%') and (lower(sessionManualAdContent) like '%buseinss%' and lower(sessionManualAdContent) like 'desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) like '%business%' and lower(site_name) like '%desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) AND (LOWER(sessionCampaignName) like '%acast%' or lower(sessionManualAdContent) like '%acast%') THEN 'Acast'
                WHEN (LOWER(sessionCampaignName) LIKE '%perf%' and lower(sessionCampaignName) like '%max%') or lower(sessionCampaignName) like '%pmax%' THEN 'Performance Max'
                WHEN LOWER(site_name) LIKE '%google_ads_search%' THEN 'Search'
                ELSE INITCAP(sessionSourceMedium)
            END AS publisher 
            FROM channels
  ),
  campaign_base AS (
       SELECT * except(sessionManualAdContent),
       CASE WHEN 
       REGEXP_REPLACE(LOWER(replace(sessionManualAdContent,'"','')),r'\s+', '') like '%accommodation%' 
       OR REGEXP_REPLACE(LOWER(replace(sessionManualAdContent,'"','')),r'\s+', '') like '%acommodation%' 
       OR REGEXP_REPLACE(LOWER(replace(sessionManualAdContent,'"','')),r'\s+', '') like '%accomodation%' THEN 'Accommodation'
       WHEN LOWER(replace(sessionManualAdContent,'"','')) like '%aop%' THEN 'Ad On Pause'
       -- Partial matches (simple lower replace)
       WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%6s-sting%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%andreymauger%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%ariakerabs%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%ariakerebs%' OR  
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%benmarsh%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%caleb%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%evelynbarber%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%jocastatwins%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%lucymason%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%lukerush%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%maddy%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%mauracody%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%myahboston%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%robyn%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%siennabach%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%zoe%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%advocacy%' OR 
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%advoacacy%' OR
       -- Partial matches that require whitespace normalization
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%ateraapriana%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')),r'\s+','') LIKE '%siennabach%' or 
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%benwoodgates%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%maanvisch%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%oliviablakey%' OR
       REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE '%paigesch%' OR

       -- Exact matches
       lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_13062022_Video_20s Advocacy Megan') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_13062022_Video_23s Advocacy Zara') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_13062022_Video_30s Advocacy Ella') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_18072022_Video_25s Advocacy Henry') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_18072022_Video_25s Advocacy Irene') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_18072022_Video_25s Advocacy Mihirangi') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_18072022_Video_25s Advocacy Zoe') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Ella') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Henry') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Irene') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Megan') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Mihirangi') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Zara') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Zoe') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_15s Advocacy Zoe Summer') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_20s Advocacy Megan') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_25s Advocacy Henry') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_25s Advocacy Irene') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_25s Advocacy Mihirangi') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_25s Advocacy Zoe') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_30s Advocacy Ella') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_30s Advocacy Zara') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Ella') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Henry') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Irene') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Megan') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Mihirangi')
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Zara') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Zoe') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_18-video_Video_Connor Gyde')   
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_18-video_Video_Jody Bam') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_23-video_Video_Brianna') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_25-video_Video_Henry') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_25-video_Video_Kayden') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_28-video_Video_Celia') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_28-video_Video_Zoe') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_29-video_Video_Mihirangi') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_37-video_Video_Accomodation Hamilton') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_76-video_Video_Aasiya') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_Snapchat_23-video_Video_Brianna') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_Snapchat_25-video_Video_Henry') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_Snapchat_28-video_Video_Zoe') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_Snapchat_29-video_Video_Mihirangi') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_TikTok_23-video_Video_Brianna') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_TikTok_25-video_Video_Henry') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_TikTok_28-video_Video_Zoe') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_TikTok_29-video_Video_Mihirangi') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_18-video_Video_Connor Gyde') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_18-video_Video_Jody Bam') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_23-video_Video_Brianna') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_25-video_Video_Henry') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_25-video_Video_Kayden') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_28-video_Video_Celia') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_28-video_Video_Zoe') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_29-video_Video_Mihirangi') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_76-video_Video_Aasiya') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_Snapchat_23-video_Video_Brianna') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_Snapchat_25-video_Video_Henry') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_Snapchat_28-video_Video_Zoe') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_Snapchat_29-video_Video_Mihirangi') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_TikTok_23-video_Video_Brianna') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_TikTok_28-video_Video_Zoe') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_TikTok_29-video_Video_Mihirangi') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0031_SOCIAL_COSIDERATION_PROG_TikTok_25-video_Video_Henry') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_24-video_Video_Andrey Mauger') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_30-video_Video_Aria Kerabs') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_43-video_Video_Ben Marsh') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_24-video_Video_Advocacy_Andrey Mauger') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_24-video_Video_Andrey Mauger') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_43-video_Video_Ben Marsh') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_24-video_Video_Andrey Mauger')
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_30-video_Video_Aria Kerabs') 
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_43-video_Video_Ben Marsh') 
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_24-video_Video_Andrey Mauger') 
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_30-video_Video_Aria Kerabs') 
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_43-video_Video_Ben Marsh') 
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_6-video_Video_Brand_Sting') 
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_24-video_Video_Andrey Mauger') 
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_30-video_Video_Aria Kerabs') 
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_43-video_Video_Advocacy_Ben Marsh')
       OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_6-video_Video_Brand_Sting') 
       OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_24-video_Video_Andrey Mauger') 
       OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_30-video_Video_Aria Kerabs') 
       OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_43-video_Video_Ben Marsh') 
       OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_24-video_Video_Andrey Mauger') 
       OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_30-video_Video_Aria Kerabs') 
       OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_43-video_Video_Ben Marsh') 
       OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_6-video_Video_Brand_Sting') 
       THEN 'Advocacy'
       WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%appliedcomputing%' THEN 'Applied Computing'
       WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%art%' THEN 'Art'
       WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%bbfintech%' THEN 'BBFineTech'
       WHEN  lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_15-video_Video_Employability') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_15-video_Video_Excellence 2') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_15-video_Video_Excellence 1') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-brand%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_15-video_Video_Experience') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-challenge%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-community%'))
     OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-discovery%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-employability%')) 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_15-video_Video_Brand_Excellence') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-excellence%')) 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_15-video_Video_Brand_Excellence') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_15-video_Video_Brand_Excellence') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-excellence2%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-experience%')) 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-innovation%')) 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%29s-mihirangi%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%30s-brand%')) 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_30-video_Video_Brand') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_60-video_Video_The People') OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%60s-people%')) 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%6s-bumper%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%nsl-15s-brand%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%nsl-15s-employability%')) 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%nsl-15s-excellence%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%Brand%')) 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_15-video_Video_Excellence') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_15-video_Video_Employability') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_6-video_Video_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_15-video_Video_Excellence') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_15-video_Video_Employability') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_6-video_Video_Sting') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_ThreeNow_15-video_Video_Brand') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_ThreeNow_15-video_Video_Brand NSL') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_ThreeNow_30-video_Video_Brand') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_ThreeNow_60-video_Video_The People') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_TVNZ_15-video_Video_Brand') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_TVNZ_15-video_Video_Brand NSL') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_TVNZ_30-video_Video_Brand') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_TVNZ_60-video_Video_The People') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_15-video_Video_Challenge') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_15-video_Video_Community') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_15-video_Video_Discovery') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_15-video_Video_Innovation') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_6-video_Video_Bumper') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_15-video_Video_Challenge') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_15-video_Video_Community') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_15-video_Video_Discovery') 
     OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_15-video_Video_Innovation')
      OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_6-video_Video_Bumper')
      or lower(REPLACE(sessionManualAdContent, '"', '')) like '%employability%' or lower(REPLACE(sessionManualAdContent, '"', '')) like '%excellence%' 
      or lower(replace(sessionManualAdContent,'"','')) like '%experience%'
     THEN 'Brand Video'
     WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%mba%' THEN 'MBA'
     WHEN lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%(not set)%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%15s-exployability%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%200x75-logo%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%23s-bri%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%6s-people%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%lucymason%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%myahboston%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%siennabach%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Native Driving Desktop Overlay') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Targeted & Contextual Music Desktop Overlay') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Native Driving Audio') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Targeted & Contextual Music Mobile Overlay') 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Native Driving Mobile Overlay') 
    THEN 'Broad'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%business%' THEN 'Business'
    WHEN REGEXP_REPLACE(lower(REPLACE(sessionManualAdContent, '"', '')), r'\s+', '') LIKE lower(('%climatechange%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_Static_Static_Climate Change') 
    THEN 'Climate Change'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%communication%' THEN 'Communication'
    WHEN (REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%comp%') and (REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%sci%')
    THEN 'Computer Science'
    WHEN (lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%Become a leader in Education by studying one of our Masters degrees in Education.%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%ECE%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%educ%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%Start your teaching journey in 2023 Teach the subjects you love. Apply now at the University of Waikato%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%Study to become a teacher and step into a new career in as little as a year.%'))) 
    THEN 'Education'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%engineering%' THEN 'Engineering'
    when REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%environment%' and LOWER(REPLACE(sessionManualAdContent,'"','')) NOT LIKE '%planning%' THEN 'Environment'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%environment%' and LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%planning%' THEN 'Environment Planning'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%globalstudies%' THEN 'Global Studies'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%health%' THEN 'Health'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%law%' THEN 'Law'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(sessionManualAdContent,'"','')), r'\s+', '') LIKE '%lifestyle%' THEN 'LifeStyle'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%management%' THEN 'Management'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%maori%' THEN 'Maori'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%ncea%' THEN 'NCEA'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%pacific%' THEN 'Pacific'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%pharmacy%' THEN 'Pharmacy'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%psych%' THEN 'Psychology'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%scholarship%' THEN 'Scholarship'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%science%' AND LOWER(REPLACE(sessionManualAdContent,'"','')) NOT LIKE '%comp%' THEN 'Science'
    WHEN ((lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%Become a leader in Education by studying one of our Masters degrees in Education.%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%ECE%')) OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%educ%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%Start your teaching journey in 2023 Teach the subjects you love. Apply now at the University of Waikato%')) 
    OR lower(REPLACE(sessionManualAdContent, '"', '')) LIKE lower(('%Study to become a teacher and step into a new career in as little as a year.%'))) ) and 
    (LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%secondary%' or LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%secndary%')
    THEN 'Secondary Education'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%sport%' THEN 'Sports'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%midwifery%' THEN 'Midwifery'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%outdoor%' THEN 'Outdoor'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%nurs%' THEN 'Nursing'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%onshore%' THEN 'Onshore'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%social%' AND LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%science%' THEN 'Social Science'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%indoor%' THEN 'Indoors'
    when lower(replace(sessionManualAdContent,'"','')) like '%english%' then 'English'
    when REGEXP_REPLACE(lower(replace(sessionManualAdContent,'"','')), r'\s+', '') like '%forthepeople%' THEN 'For The People'
    WHEN LOWER(REPLACE(sessionManualAdContent,'"','')) LIKE '%profession%' and LOWER(REPLACE(sessionManualAdContent,'"','')) like '%accounting%' THEN 'Professional Accounting'
    else 
       case when array_length(split(sessionManualAdContent,'_')) >=8 then split(sessionManualAdContent,'_')[offset(7)]
       else sessionManualAdContent
       end
    END AS sessionManualAdContent,
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
END AS campaign_name_selection,
CASE
  WHEN EXISTS (
    SELECT 1
    FROM UNNEST(SPLIT(REGEXP_REPLACE(campaign_name, r'[-_]', '|'), '|')) AS part
    WHERE LOWER(part) LIKE '%uow0%'
  )
  THEN (
    SELECT UPPER(part)
    FROM UNNEST(SPLIT(REGEXP_REPLACE(campaign_name, r'[-_]', '|'), '|')) AS part
    WHERE LOWER(part) LIKE '%uow0%'
    LIMIT 1
  )
  ELSE 'Other'
END AS campaign_name_indication


 FROM campaign_base camb LEFT JOIN deduplicate_raw ON LOWER(deduplicate_raw.campaign_name_raw) = LOWER(camb.campaign_name_raw)
),
funnel_campaign AS (
     select distinct funnel, campaign_name from `uowaikato-main.dash_table.dash_union`
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
non_media_format as (

select 
COALESCE(t2.new_campaign_name,t1.campaign_name) AS campaign_name,
t1.* except(campaign_name)
from semi_final as t1 left join `together-internal.google_ads_campaign_mapping.campaign_name_mapping` as t2
on lower(trim(t1.campaign_name)) = lower(trim(t2.old_campaign_name)))
SELECT md.*,
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

  WHEN LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%google%' AND
       LOWER(COALESCE(sessionSourceMediumraw,'')) LIKE '%cpc%' AND
       LOWER(COALESCE(md.campaign_name,''))    LIKE '%wat%'
  THEN 'DEMAND GEN'

  ELSE 'OTHER'
END AS media_format,

FROM non_media_format as md left join funnel_campaign AS fc on fc.campaign_name = md.campaign_name