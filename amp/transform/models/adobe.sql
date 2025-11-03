{{ config(
    materialized='table',
) }}
WITH base AS (
  SELECT
    PARSE_DATE('%B %e, %Y', _Date_) AS date,
    `Tracking Code`,
    SPLIT(`Tracking Code`,':') AS parts,


    Visits,
    `Unique Visitors` AS unique_visitors,
    `KiwiSaver 2:Form Start _event180_` AS kiwisaver_form_start,
    `KiwiSaver 2:Form Complete _event185_` AS kiwisaver_form_complete,
    `MF:Prospect:Join MF: Cloud Pg Form Start _e299_ _event299_` AS mf_form_start,
    `MF:Prospect:Join MF: Cloud Pg Form Complete _e300_ _event300_` AS mf_form_complete,
    `TD:Prospect:Join TD: Cloud Pg Form Start _e266_ _event266_` AS td_form_start,
    `TD:Prospect:Join TD: Cloud Pg Form Complete _e267_ _event267_` AS td_form_complete,
    `GI:Quote Started _e205_ _event205_` AS gi_quote_start,
    `GI:Quote Completed _e206_ _event206_` AS gi_quote_complete,
    `GI:Buy Started _e210_ _event210_` AS gi_buy_start,
    `GI:Buy Completed _e211_ _event211_` AS gi_buy_complete
  FROM `amp-main.adobe_raw.adobe`
  
),
define_source AS (
  SELECT *,
    TRIM(parts[SAFE_OFFSET(0)]) AS Source,
    TRIM(parts[SAFE_OFFSET(1)]) AS Medium,
    TRIM(parts[SAFE_OFFSET(2)]) AS Campaign,
    TRIM(parts[SAFE_OFFSET(3)]) AS Ad_Content,
    TRIM(parts[SAFE_OFFSET(4)]) AS Term
    FROM base
),
-- map UTM paramters to site name
site_classified AS (
  SELECT *,
    CASE
      WHEN NULLIF(TRIM(`Tracking Code`), '') IS NULL THEN 'Direct'
      WHEN Source = 'sem' AND Medium = 'google' AND NOT LOWER(Campaign) LIKE '%demand-gen%' AND NOT LOWER(Campaign) LIKE '%performance_max%' AND NOT LOWER(Campaign) LIKE '%pmax%' THEN 'Search'
      WHEN Source = 'sem' AND Medium = 'google' AND (LOWER(Campaign) LIKE '%performance_max%' or lower(Campaign) LIKE '%pmax%') THEN 'Performance Max'
      WHEN Source = 'sem' AND Medium = 'google' AND LOWER(Campaign) LIKE '%demand-gen%' THEN 'Demand Gen'
      WHEN (Source = 'sem' AND Medium = 'bing') OR Source = 'bing' OR Medium = 'bing' THEN 'Bing Ads Search'
      WHEN Source = 'Unspecified' THEN 'Other Owned and Organic Combined'
      WHEN Source = 'email' THEN 'Email'
      WHEN (Source = 'social' AND Medium = 'reddit')  THEN 'Reddit'
      WHEN (trim(Source) = 'social' AND trim(Medium) = 'meta') OR LOWER(Campaign) like '%meta%' THEN 'Meta'
      WHEN Source = 'affiliate' THEN 'Affiliate'
      WHEN Medium IN ('3now', 'stuff', 'canstar') THEN INITCAP(Medium)
      WHEN Medium IN ('tvnz','ttd','nzme') THEN UPPER(Medium)
      WHEN Medium IN ('dv360') THEN 'DV360'

      WHEN Medium = 'poster' OR Source = 'poster' THEN 'Poster'
      WHEN Medium = 'portal' OR Source = 'portal' OR LOWER(TRIM(Medium)) LIKE '%myamp%' THEN 'My Amp Portal'
      WHEN LOWER(TRIM(Medium)) = 'linkedin' THEN 'LinkedIn'
      WHEN LOWER(TRIM(Medium)) like '%youtube%' THEN 'YouTube'
      WHEN LOWER(TRIM(Medium)) like '%nzme%' THEN 'NZME'
      ELSE 'Other'
    END AS Site_Name
  FROM define_source
),

-- map campaign names
campaign_named AS (
  SELECT *,
    CASE
      WHEN NULLIF(TRIM(`Tracking Code`), '') IS NULL THEN 'direct'
      WHEN Site_Name = 'Google Ads Search' THEN CONCAT('Google - Ads - Search - ', INITCAP(Campaign), '_', Ad_Content)
      WHEN Site_Name = 'Performance Max' THEN CONCAT('Google - Ads - Performance Max - ', INITCAP(Campaign), '_', Ad_Content)
      WHEN Site_Name = 'Bing Ads Search' THEN CONCAT('Bing - Ads - Search - ', INITCAP(Campaign), '_', Ad_Content)
      WHEN Site_Name = 'Demand Gen' AND Campaign != 'demand-gen-investments' THEN CONCAT('amp0_', Campaign)
      WHEN Campaign = 'demand-gen-investments' THEN 'amp0_demand-gen-investments-td'
      ELSE Campaign
    END AS cleaned_campaign_name
  FROM site_classified
),

-- unpivot table to establish similar format as GA4 datasets
unpivoted AS (
  SELECT date, Source, Medium, Campaign AS sessionCampaignName, Ad_Content as sessionManualAdContent, Term, Site_Name as site_name, cleaned_campaign_name as campaign_name, `Tracking Code`,
         'visits' AS eventName, Visits AS eventCount FROM campaign_named WHERE Visits IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'unique_visitors', unique_visitors FROM campaign_named WHERE unique_visitors IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'kiwisaver_form_start', kiwisaver_form_start FROM campaign_named WHERE kiwisaver_form_start IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'kiwisaver_form_complete', kiwisaver_form_complete FROM campaign_named WHERE kiwisaver_form_complete IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'mf_form_start', mf_form_start FROM campaign_named WHERE mf_form_start IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'mf_form_complete', mf_form_complete FROM campaign_named WHERE mf_form_complete IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'td_form_start', td_form_start FROM campaign_named WHERE td_form_start IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'td_form_complete', td_form_complete FROM campaign_named WHERE td_form_complete IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'gi_quote_start', gi_quote_start FROM campaign_named WHERE gi_quote_start IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'gi_quote_complete', gi_quote_complete FROM campaign_named WHERE gi_quote_complete IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'gi_buy_start', gi_buy_start FROM campaign_named WHERE gi_buy_start IS NOT NULL
  UNION ALL
  SELECT date, Source, Medium, Campaign, Ad_Content, Term, Site_Name, cleaned_campaign_name, `Tracking Code`, 'gi_buy_complete', gi_buy_complete FROM campaign_named WHERE gi_buy_complete IS NOT NULL
),
sub_brands as (
SELECT *,
CASE
WHEN NULLIF(TRIM(`Tracking Code`), '') IS NULL THEN 'direct'
WHEN REGEXP_CONTAINS(LOWER(sessionCampaignName),r'(^|[ _])gi($|[ _])')  or lower(sessionCampaignName) like '%general insurance%' OR LOWER(sessionCampaignName) like '%generalinsurance%' 
or REGEXP_CONTAINS(LOWER(sessionCampaignName),r'(^|[ _])car($|[ _])') or lower(sessionCampaignName) like '%home%' or lower(sessionCampaignName) like '%content%'
THEN 'General Insurance'
ELSE 'Wealth'
END AS sub_brands,
site_name AS publisher

FROM unpivoted),
publisher_channel as (
SELECT sub.* EXCEPT(publisher),
COALESCE(sub.publisher,channel.publisher) AS publisher,
channel.channel
FROM sub_brands as sub LEFT JOIN `together-internal.channel.publisher_channel` as channel
ON lower(sub.publisher) = lower(channel.publisher)
)

SELECT * EXCEPT(Date,channel,campaign_name), Date as date,
CASE WHEN publisher = 'Direct' THEN 'Direct' ELSE channel END AS channel,
CASE WHEN campaign_name is null or campaign_name = '' then 'Other' ELSE campaign_name END AS campaign_name,
CASE WHEN publisher = 'Direct' THEN 'Direct' WHEN campaign_name is null or campaign_name = '' THEN 'Other'ELSE campaign_name END AS campaign_name_selection FROM publisher_channel
