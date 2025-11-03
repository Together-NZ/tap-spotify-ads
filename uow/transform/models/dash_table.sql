{{ config(
    materialized='table',
) }}



WITH dash_table AS (
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_views,
           campaign_name, publisher, campaign_descr,  date(date) as date
    FROM `uowaikato-main.ttd_transformed.ttd_transformed`

    UNION ALL

    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr,  date(date) as date
    FROM `uowaikato-main.dv360_transformed.dv360_standard` WHERE LOWER(campaign_name) NOT LIKE '%yt%'
    
    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr,  date(date) as date
    FROM `uowaikato-main.dv360_transformed.dv360_youtube` 
    UNION ALL

    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion
           ,video_25_completion,video_50_completion,video_75_completion, video_views,
           campaign_name, publisher, campaign_descr,  date(date) as date
    FROM `uowaikato-main.tiktok_transformed.tiktok_ads_join`

    UNION ALL

    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_views,
           campaign_name, publisher, campaign_descr,  date(date) as date
    FROM `uowaikato-main.snapchat_transformed.snapchat_ads`

    UNION ALL
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_views,
           campaign_name, publisher, campaign_descr, date(date) as date
    FROM `uowaikato-main.linkedin_transformed.linkedin`

    UNION ALL
    -- Handling Google Ads arrays by converting them to strings
        SELECT media_cost, impressions,clicks,
              ad_name AS  creative_name,  
                     --ARRAY_TO_STRING(media_format, ', ') AS media_format,   -- Convert array to string
                     audience_name, -- Convert array to string
                     ad_format,         -- Convert array to string
                     ad_format_detail, 
                     CAST(0 AS INT64) AS video_completion,
                     CAST(0 AS INT64) AS video_25_completion,
                     CAST(0 AS INT64) AS video_50_completion,
                     CAST(0 AS INT64) AS video_75_completion,
                     CAST(0 AS INT64) AS video_views,
                     
                     campaign_name,publisher, campaign_descr, 
 -- Convert array to string
                     date,

    FROM `uowaikato-main.google_ads_transformed.google_ads`

    UNION ALL

    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_played AS video_views,
           campaign_name, publisher, campaign_descr,  date(date) as date
    FROM `uowaikato-main.facebook_transformed.facebook`

    UNION ALL
    select media_cost,impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, 
        CAST(0 AS INT64) AS video_completion,
        CAST(0 AS INT64) AS video_25_completion,
        CAST(0 AS INT64) AS video_50_completion,
        CAST(0 AS INT64) AS video_75_completion,
        CAST(0 AS INT64) AS video_views,
    campaign_name,  publisher, campaign_descr,  date(date) as date,

from `uowaikato-main.cm360_transformed.cm360_direct_buy`
),
with_channel as (
SELECT * EXCEPT (publisher,channel), 
dc.publisher,
dc.channel

FROM dash_table as dt join `together-internal.channel.publisher_channel` as dc
ON lower(trim(dt.publisher)) = lower(trim(dc.publisher))),
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
)
SELECT camb.* EXCEPT(campaign_name_raw),
0 as metrics_value_per_conversion,
NULL AS segments_conversion_action,
NULL AS segments_conversion_action_category,
NULL AS segments_conversion_action_name,
NULL AS segments_conversion_attribution_event_type,
NULL AS segments_day_of_week,
NULL AS segments_month,
NULL AS segments_week,
NULL AS segments_quarter,
NULL AS segments_year,
NULL AS bidding_strategy_name,
NULL AS campaign_advertising_channel_sub_type,
NULL AS campaign_advertising_channel_type,
NULL AS campaign_bidding_strategy,
NULL AS campaign_bidding_strategy_type,
NULL AS campaign_budget_amount_micros,
NULL AS campaign_budget_explicitly_shared,
NULL AS campaign_budget_has_recommended_budget,
NULL AS campaign_budget_period,
NULL AS campaign_budget_recommended_budget_amount_micros,
NULL AS campaign_budget_total_amount_micros,
NULL AS campaign_campaign_budget,
NULL AS campaign_end_date,
NULL AS campaign_experiment_type,
NULL AS campaign_manual_cpc_enhanced_cpc_enabled,
NULL AS campaign_maximize_conversion_value_target_roas,
NULL AS campaign_percent_cpc_enhanced_cpc_enabled,
NULL AS campaign_serving_status,
NULL AS campaign_start_date,
NULL AS campaign_status,
NULL AS campaign_tracking_url_template,
NULL AS campaign_url_custom_parameters,
NULL AS campaign_id,
NULL AS customer_id,
NULL AS campaign_base_campaign,
NULL AS metrics_conversions,
NULL AS metrics_conversions_value,
NULL AS metrics_interaction_event_types,
NULL AS metrics_interactions,
NULL AS metrics_view_through_conversions,
NULL AS segments_ad_network_type,
NULL AS segments_device,
NULL AS segments_slot,
NULL AS _LATEST_DATE,
NULL AS _DATA_DATE,
trim(CASE WHEN 
       lower(camb.campaign_name_raw) = lower(deduplicate_raw.campaign_name_raw) 
       
       THEN deduplicate_raw.campaign_name_raw
       ELSE camb.campaign_name_raw
END )AS campaign_name_selection,
CASE WHEN 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(creative_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(creative_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       WHEN  EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       else 'Other'
END as media_format,
CASE WHEN 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_')) as a
       WHERE LOWER(a) IN UNNEST(ARRAY['consideration','awareness','intent']))
       THEN (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE LOWER(X) IN UNNEST(['consideration','awareness','intent'])
       LIMIT 1)
       else 'Other'
END AS funnel,
CASE WHEN 
       REGEXP_REPLACE(LOWER(replace(creative_name,'"','')),r'\s+', '') like '%accommodation%' 
       OR REGEXP_REPLACE(LOWER(replace(creative_name,'"','')),r'\s+', '') like '%acommodation%'
       OR REGEXP_REPLACE(LOWER(replace(creative_name,'"','')),r'\s+', '') like '%accomodation%' THEN 'Accommodation'
       WHEN LOWER(replace(creative_name,'"','')) like '%aop%' THEN 'Ad On Pause'
       -- Partial matches (simple lower replace)
       WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%6s-sting%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%andreymauger%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%ariakerabs%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%ariakerebs%' OR  
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%benmarsh%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%caleb%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%evelynbarber%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%jocastatwins%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%lucymason%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%lukerush%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%maddy%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%mauracody%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%myahboston%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%robyn%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%siennabach%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%zoe%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%advocacy%' OR 
       REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%advoacacy%' OR

       -- Partial matches that require whitespace normalization
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%ateraapriana%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')),r'\s+','') LIKE '%siennabach%' or 
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%benwoodgates%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%maanvisch%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%oliviablakey%' OR
       REGEXP_REPLACE(LOWER(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE '%paigesch%' OR

       -- Exact matches
       lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_13062022_Video_20s Advocacy Megan') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_13062022_Video_23s Advocacy Zara') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_13062022_Video_30s Advocacy Ella') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_18072022_Video_25s Advocacy Henry') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_18072022_Video_25s Advocacy Irene') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_18072022_Video_25s Advocacy Mihirangi') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_FB_AP 16-24_FBINS_18072022_Video_25s Advocacy Zoe') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Ella') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Henry') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Irene') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Megan') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Mihirangi') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Zara') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_AWR_PRO_TIKTOK_AP 16-19_TIKTOK_13062022_Video_Advocacy Zoe') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_15s Advocacy Zoe Summer') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_20s Advocacy Megan') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_25s Advocacy Henry') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_25s Advocacy Irene') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_25s Advocacy Mihirangi') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_25s Advocacy Zoe') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_30s Advocacy Ella') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_FB_AP 18-24+Interest_FBINS_18072022_Video_30s Advocacy Zara') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Ella') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Henry') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Irene') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Megan') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Mihirangi')
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Zara') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0020_Traffic_CON_PRO_TIKTOK_AP 16-24+Interest_TIKTOK_18072022_Video_Advocacy Zoe') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_18-video_Video_Connor Gyde') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_18-video_Video_Jody Bam') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_23-video_Video_Brianna') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_25-video_Video_Henry') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_25-video_Video_Kayden') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_28-video_Video_Celia') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_28-video_Video_Zoe') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_29-video_Video_Mihirangi') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_37-video_Video_Accomodation Hamilton') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_META_76-video_Video_Aasiya') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_Snapchat_23-video_Video_Brianna') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_Snapchat_25-video_Video_Henry') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_Snapchat_28-video_Video_Zoe') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_Snapchat_29-video_Video_Mihirangi') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_TikTok_23-video_Video_Brianna') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_TikTok_25-video_Video_Henry') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_TikTok_28-video_Video_Zoe') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_AWARENESS_PROG_TikTok_29-video_Video_Mihirangi') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_18-video_Video_Connor Gyde') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_18-video_Video_Jody Bam') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_23-video_Video_Brianna') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_25-video_Video_Henry') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_25-video_Video_Kayden') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_28-video_Video_Celia') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_28-video_Video_Zoe') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_29-video_Video_Mihirangi') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_META_76-video_Video_Aasiya') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_Snapchat_23-video_Video_Brianna') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_Snapchat_25-video_Video_Henry') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_Snapchat_28-video_Video_Zoe') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_Snapchat_29-video_Video_Mihirangi') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_TikTok_23-video_Video_Brianna') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_TikTok_28-video_Video_Zoe') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_CONSIDERATION_PROG_TikTok_29-video_Video_Mihirangi') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0031_SOCIAL_COSIDERATION_PROG_TikTok_25-video_Video_Henry') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_24-video_Video_Andrey Mauger') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_30-video_Video_Aria Kerabs') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_43-video_Video_Ben Marsh') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_24-video_Video_Advocacy_Andrey Mauger') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_24-video_Video_Andrey Mauger') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_43-video_Video_Ben Marsh') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_24-video_Video_Andrey Mauger')
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_30-video_Video_Aria Kerabs') 
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_43-video_Video_Ben Marsh') 
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_24-video_Video_Andrey Mauger') 
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_30-video_Video_Aria Kerabs') 
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_43-video_Video_Ben Marsh') 
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_6-video_Video_Brand_Sting') 
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_24-video_Video_Andrey Mauger') 
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_30-video_Video_Aria Kerabs') 
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_43-video_Video_Advocacy_Ben Marsh')
       OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_6-video_Video_Brand_Sting') 
       OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_24-video_Video_Andrey Mauger') 
       OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_30-video_Video_Aria Kerabs') 
       OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_43-video_Video_Ben Marsh') 
       OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_24-video_Video_Andrey Mauger') 
       OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_30-video_Video_Aria Kerabs') 
       OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_43-video_Video_Ben Marsh') 
       OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_6-video_Video_Brand_Sting') 
       OR LOWER(creative_name) LIKE 'nikki' OR LOWER(creative_name) like '%lloyd'
       THEN 'Advocacy'
       WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%appliedcomputing%' THEN 'Applied Computing'
       WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%art%' THEN 'Art'
       WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%bbfintech%' THEN 'BBFineTech'
       WHEN  lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_15-video_Video_Employability') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_15-video_Video_Excellence 2') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_15-video_Video_Excellence 1') 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-brand%')) 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_15-video_Video_Experience') 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-challenge%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-community%'))
     OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-discovery%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-employability%')) 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_15-video_Video_Brand_Excellence') 
     OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-excellence%')) 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_15-video_Video_Brand_Excellence') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_15-video_Video_Brand_Excellence') 
     OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-excellence2%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-experience%')) 
     OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-innovation%')) 
     OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%29s-mihirangi%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%30s-brand%')) 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_30-video_Video_Brand') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_Multi_60-video_Video_The People') OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%60s-people%')) 
     OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%6s-bumper%')) OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_INTENT_PROG_META_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%nsl-15s-brand%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%nsl-15s-employability%')) 
     OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%nsl-15s-excellence%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%Brand%')) 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_META_15-video_Video_Excellence') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_15-video_Video_Employability') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_TIKTOK_6-video_Video_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_META_15-video_Video_Excellence') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_15-video_Video_Brand_Employability') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_SNAPCHAT_6-video_Video_Brand_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_15-video_Video_Employability') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_CONSIDERATION_PROG_TIKTOK_6-video_Video_Sting') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_ThreeNow_15-video_Video_Brand') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_ThreeNow_15-video_Video_Brand NSL') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_ThreeNow_30-video_Video_Brand') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_ThreeNow_60-video_Video_The People') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_TVNZ_15-video_Video_Brand') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_TVNZ_15-video_Video_Brand NSL') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_TVNZ_30-video_Video_Brand') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_VIDOD_AWARENESS_PROG_TVNZ_60-video_Video_The People') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_15-video_Video_Challenge') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_15-video_Video_Community') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_15-video_Video_Discovery') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_15-video_Video_Innovation') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_AWARENESS_PROG_Google_6-video_Video_Bumper') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_15-video_Video_Challenge') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_15-video_Video_Community') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_15-video_Video_Discovery') 
     OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_15-video_Video_Innovation')
      OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_YT_CONSIDERATION_PROG_Google_6-video_Video_Bumper')
      or lower(REPLACE(creative_name, '"', '')) like '%employability%' or lower(REPLACE(creative_name, '"', '')) like '%excellence%' 
      or lower(replace(creative_name,'"','')) like '%experience%'
      OR lower(creative_name) = lower('UOW0052_SOCIAL_CONSIDERATION_PROG_Linkedin_5"-video_Video_MBA')
     THEN 'Brand Video'
     WHEN lower(REPLACE(creative_name, '"', '')) LIKE lower(('%(not set)%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%15s-exployability%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%200x75-logo%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%23s-bri%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%6s-people%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%lucymason%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%myahboston%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%siennabach%')) OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Native Driving Desktop Overlay') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Targeted & Contextual Music Desktop Overlay') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Native Driving Audio') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Targeted & Contextual Music Mobile Overlay') 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_TRI A 2025_AWARENESS_Spotify_Native Driving Mobile Overlay') 
    THEN 'Broad'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%mba%' THEN 'MBA'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%business%' THEN 'Business'
    WHEN REGEXP_REPLACE(lower(REPLACE(creative_name, '"', '')), r'\s+', '') LIKE lower(('%climatechange%')) 
    OR lower(REPLACE(creative_name, '"', '')) = lower('UOW0042_SOCIAL_AWARENESS_PROG_SNAPCHAT_Static_Static_Climate Change') 
    THEN 'Climate Change'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%communication%' THEN 'Communication'
    WHEN (REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%comp%') and (REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%sci%')
    THEN 'Computer Science'
    WHEN (lower(REPLACE(creative_name, '"', '')) LIKE lower(('%Become a leader in Education by studying one of our Masters degrees in Education.%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%ECE%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%educ%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%Start your teaching journey in 2023 Teach the subjects you love. Apply now at the University of Waikato%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%Study to become a teacher and step into a new career in as little as a year.%'))) 
    THEN 'Education'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%engineering%' THEN 'Engineering'
    when REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%environment%' and LOWER(REPLACE(creative_name,'"','')) NOT LIKE '%planning%' THEN 'Environment'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%environment%' and LOWER(REPLACE(creative_name,'"','')) LIKE '%planning%' THEN 'Environment Planning'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%globalstudies%' THEN 'Global Studies'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%health%' THEN 'Health'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%law%' THEN 'Law'
    WHEN REGEXP_REPLACE(LOWER(REPLACE(creative_name,'"','')), r'\s+', '') LIKE '%lifestyle%' THEN 'LifeStyle'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%management%' THEN 'Management'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%maori%' THEN 'Maori'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%ncea%' THEN 'NCEA'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%pacific%' THEN 'Pacific'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%pharmacy%' THEN 'Pharmacy'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%psych%' THEN 'Psychology'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%scholarship%' THEN 'Scholarship'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%science%' AND LOWER(REPLACE(creative_name,'"','')) NOT LIKE '%comp%' THEN 'Science'
    WHEN ((lower(REPLACE(creative_name, '"', '')) LIKE lower(('%Become a leader in Education by studying one of our Masters degrees in Education.%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%ECE%')) OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%educ%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%Start your teaching journey in 2023 Teach the subjects you love. Apply now at the University of Waikato%')) 
    OR lower(REPLACE(creative_name, '"', '')) LIKE lower(('%Study to become a teacher and step into a new career in as little as a year.%'))) ) and 
    (LOWER(REPLACE(creative_name,'"','')) LIKE '%secondary%' or LOWER(REPLACE(creative_name,'"','')) LIKE '%secndary%')
    THEN 'Secondary Education'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%sport%' THEN 'Sports'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%midwifery%' THEN 'Midwifery'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%outdoor%' THEN 'Outdoor'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%nurs%' THEN 'Nursing'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%onshore%' THEN 'Onshore'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%social%' AND LOWER(REPLACE(creative_name,'"','')) LIKE '%science%' THEN 'Social Science'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%indoor%' THEN 'Indoors'
    when lower(replace(creative_name,'"','')) like '%english%' then 'English'
    when REGEXP_REPLACE(lower(replace(creative_name,'"','')), r'\s+', '') like '%forthepeople%' THEN 'For The People'
    WHEN LOWER(REPLACE(creative_name,'"','')) LIKE '%profession%' and LOWER(REPLACE(creative_name,'"','')) like '%accounting%' THEN 'Professional Accounting'
    else 
       case when array_length(split(creative_name,'_')) >=8 then split(creative_name,'_')[offset(7)]
       else creative_name
       end
    END AS creative_descr

 FROM campaign_base camb LEFT JOIN deduplicate_raw ON LOWER(deduplicate_raw.campaign_name_raw) = LOWER(camb.campaign_name_raw)




