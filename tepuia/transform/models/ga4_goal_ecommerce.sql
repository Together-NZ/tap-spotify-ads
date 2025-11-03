{{ config(
    materialized='table',
) }}
WITH ecommerce AS (
    SELECT JSON_VALUE(data,'$.sessionCampaignName') AS campaign_name,
    JSON_VALUE(data, '$.itemName') AS product_name,
    JSON_VALUE(data, '$.sessionSourceMedium') AS sessionSourceMedium,
    PARSE_DATE('%Y%m%d', JSON_VALUE(data, '$.date')) AS date,
    SAFE_CAST(JSON_VALUE(data,'$.itemRevenue') AS FLOAT64) AS itemRevenue,
    SAFE_CAST(JSON_VALUE(data,'$.itemsPurchased') AS INT64) AS itemsPurchased,
    JSON_VALUE(data,'$.sessionManualAdContent') AS sessionManualAdContent,
    _sdc_extracted_at,
    JSON_VALUE(data,'$.itemVariant') AS itemVariant,
    CASE 
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%organic%' THEN 'organic_search'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%direct%' 
              AND JSON_VALUE(data, '$.sessionCampaignName') NOT LIKE 'wat-' THEN 'direct'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%email%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%mailout%' 
              OR (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%automated%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%email%') THEN 'email'
        WHEN (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%social%')
              AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'facebook'
        WHEN JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%search%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%sem%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%performance max%'
                  or lower(json_value(data, '$.sessionCampaignName')) like '%pmax%')
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%google / cpc%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%_uow0%'
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%wat-%') THEN 'google_ads_search'
        WHEN (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%googleads%' 
              OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%googleads%' 
              OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%native%' 
              OR (LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%demand%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%gen%'))
            OR (JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
                  AND JSON_VALUE(data, '$.sessionCampaignName') LIKE 'wat-%')
            OR (JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
                  AND JSON_VALUE(data, '$.sessionCampaignName') LIKE '_uow%'
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%sem%'
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%search%') THEN 'demand_gen'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%kargo%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'kargo'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'linkedin'
        WHEN ((LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%')
              AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%cpm%' 
              AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%cpc%')
            OR ((LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%')
                  AND JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%referral%') THEN 'own_social'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%referral%' THEN 'referral'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%snapchat%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'snapchat'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%spotify%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'spotify'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%stuff%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'stuff'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%tiktok%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'tiktok'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'twitter'
        ELSE 'other'
    END AS site_name
    FROM `tepuia-main.ga4_raw.ecommerce_goal`
),
deduplicated_data AS (
    select *,
    
    ROW_NUMBER() OVER (PARTITION BY campaign_name,product_name,sessionSourceMedium,date,sessionManualAdContent,site_name,itemVariant ORDER BY _sdc_extracted_at DESC) as row_num
    from ecommerce 
),
channel_data AS (
    select * ,
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
        WHEN LOWER(site_name) LIKE '%meta%' and (lower(campaign_name) like '%wat%' or lower(split(campaign_name,'_')[safe_offset(0)]) like '%00%') THEN 'Paid Social'
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
        WHEN LOWER(site_name) LIKE '%own_social%' or ((lower(campaign_name) not like '%wat%' or lower(split(campaign_name,'_')[safe_offset(0)]) not like '%00%') and lower(site_name)='meta') THEN 'Owned Social'
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
    FROM deduplicated_data
)

SELECT * ,
CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) >=2 
THEN SPLIT(campaign_name,'_')[OFFSET(1)] 
ELSE campaign_name
END AS campaign_name_selection,
 CASE 
                WHEN LOWER(site_name) like '%own_social%' THEN 'Owned Social'
                WHEN LOWER(site_name) LIKE '%meta%' AND (lower(campaign_name) like '%wat%' or lower(split(campaign_name,'_')[safe_offset(0)]) like '%00%') and channel like '%Paid%' THEN 'Meta'
                WHEN LOWER(site_name) LIKE '%demand_gen%' THEN 'Demand Gen'
                WHEN LOWER(site_name) LIKE '%organic_search' THEN 'Organic Search'
                WHEN LOWER(sessionSourceMedium) LIKE '%ttd%' THEN 'Ttd'
                WHEN LOWER(site_name) LIKE '%email%' THEN 'Email'
                WHEN LOWER(site_name) LIKE '%tiktok%' AND (lower(campaign_name) like '%wat%' or lower(split(campaign_name,'_')[safe_offset(0)]) like '%00%') and channel like '%Paid%' THEN 'Tiktok'
                WHEN LOWER(sessionSourceMedium) LIKE '%snapchat%' THEN 'Snapchat'
                WHEN LOWER(sessionSourceMedium) LIKE '%youtube%' OR LOWER(sessionSourceMedium) LIKE '%yt%' or lower(campaign_name) like '%yt%' then 'Youtube'
                WHEN LOWER(sessionSourceMedium) LIKE '%3now%' OR LOWER(sessionSourceMedium) LIKE '%three%' THEN 'Threenow'
                WHEN LOWER(sessionSourceMedium) LIKE '%nzme%' THEN 'Nzme'
                WHEN LOWER(sessionSourceMedium) LIKE '%acast%' THEN 'Acast'
                WHEN (lower(site_name) like '%referral%') THEN 'Referral'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) and (lower(campaign_name) like '%youtube%'
                or lower(campaign_name) like '%yt%' or lower(sessionManualAdContent) like '%yt%' or lower(sessionManualAdContent) like '%youtube%') THEN 'Youtube'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) and (lower(campaign_name) like '%nzme%' 
                or lower(sessionManualAdContent) like '%nzme%') THEN 'Nzme'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) and (lower(campaign_name) like '%tvnz%' or lower(sessionManualAdContent)
                like '%tvnz%') THEN 'Tvnz'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) and (lower(campaign_name) like '%3now%' or lower(sessionManualAdContent)
                like '%three%' or lower(sessionManualAdContent) like '%3now%' or lower(campaign_name) like '%three%') THEN 'Threenow'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%') and (lower(campaign_name) like '%stuff%' or lower(sessionManualAdContent) like '%stuff%' )
                THEN 'Stuff'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name)like '%metservice%') and (lower(campaign_name) like '%metservice%' or lower(sessionManualAdContent) like '%metservice%') THEN 'Metservice'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name)like '%mediaworks%') and (lower(campaign_name) like '%mediaworks%' or lower(sessionManualAdContent) like '%mediaworks%') THEN 'Mediaworks'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name) like '%nzme%') and (lower(campaign_name) like '%buseinss%' and lower(campaign_name) like 'desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) LIKE '%dv360%' or lower(site_name) like '%nzme%') and (lower(sessionManualAdContent) like '%buseinss%' and lower(sessionManualAdContent) like 'desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) like '%business%' and lower(site_name) like '%desk%') THEN 'Business Desk'
                WHEN (LOWER(site_name) like '%ttd%' or lower(site_name) like '%dv360%' ) AND (LOWER(campaign_name) like '%acast%' or lower(sessionManualAdContent) like '%acast%') THEN 'Acast'
                WHEN (LOWER(campaign_name) LIKE '%perf%' and lower(campaign_name) like '%max%') or lower(campaign_name) like '%pmax%' THEN 'Performance Max'
                WHEN LOWER(site_name) LIKE '%google_ads_search%' THEN 'Search'
                ELSE TRIM(INITCAP(sessionSourceMedium))
            END AS publisher
FROM channel_data
WHERE row_num = 1