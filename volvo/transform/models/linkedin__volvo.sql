{{ config(
    materialized='table',
) }}
WITH final as (
SELECT creative.creative_name ,
base.*


FROM {{ref ('linkedin_no_creative__volvo')}} AS base LEFT JOIN 
 `together-internal.linkedin_transformed.linkedin_naming` AS creative
ON SAFE_CAST(creative.creative_id AS STRING) = base.creative_id
)
select *, 
SPLIT(campaign_name,'_')[OFFSET(1)] AS campaign_descr,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(campaign_name, '_')) >= 7 THEN SPLIT(campaign_name, '_')[OFFSET(7)] 
  ELSE NULL
END AS audience_name,
CASE 
        WHEN SPLIT (campaign_name,'_')[OFFSET(2)] LIKE '%SOCIAL%'
        AND (
            lower(creative_name) LIKE '%vid%'
            OR lower(campaign_name) LIKE '%vid%'
        ) THEN 'Social Video'
        WHEN SPLIT (campaign_name,'_')[OFFSET(2)] LIKE '%SOCIAL%'
        AND (
            lower(creative_name) NOT LIKE '%vid%'
            AND lower(campaign_name) NOT LIKE '%vid%'
        )
        THEN 'Social Display'
        ELSE 'Other'
  END AS media_format,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(campaign_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[OFFSET(5)] 
  ELSE NULL
END AS ad_format_detail,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(campaign_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[OFFSET(ARRAY_LENGTH(SPLIT(creative_name, '_')) -2 )] 
  ELSE NULL
END AS ad_format,
CASE 
  WHEN ARRAY_LENGTH(SPLIT(campaign_name, '_')) >= 7 THEN SPLIT(creative_name, '_')[OFFSET(ARRAY_LENGTH(SPLIT(creative_name, '_')) -1 )] 
  ELSE NULL
END AS creative_descr,
'Linkedin' as publisher
FROM final