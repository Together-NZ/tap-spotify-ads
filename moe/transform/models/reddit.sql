{{ config(
    materialized='table',
) }}
with ads as (
    select DISTINCT JSON_VALUE(data,'$.id') as ad_id,
    JSON_VALUE(data,'$.campaign_id') as campaign_id,
    JSON_VALUE(data,'$.name') as ad_name
    from `moe-main.reddit_raw.ads`
),
campaigns as (
    select distinct JSON_VALUE(data,'$.id') as campaign_id,
    JSON_VALUE(data,'$.name') as campaign_name
    from `moe-main.reddit_raw.campaigns`
),
initial_data as (
    select JSON_VALUE(data,'$.ad_id') as ad_id,
    JSON_VALUE(data,'$.campaign_id') as campaign_id,
    CAST(DATETIME(TIMESTAMP(JSON_VALUE(data,'$.date')), 'Pacific/Auckland') AS DATE) as date,
    _sdc_extracted_at as extracted_at,
    SAFE_CAST(JSON_VALUE(data,'$.spend') AS FLOAT64) as media_cost,
    SAFE_CAST(JSON_VALUE(data,'$.impressions') AS INT64) as impressions,
    SAFE_CAST(JSON_VALUE(data,'$.clicks') AS INT64) as clicks,
    SAFE_CAST(JSON_VALUE(data,'$.video_started') AS INT64) as video_started,
    SAFE_CAST(JSON_VALUE(data,'$.video_watched_25_percent') AS INT64) as video_25_completion,
    SAFE_CAST(JSON_VALUE(data,'$.video_watched_50_percent') AS INT64) as video_50_completion,
    SAFE_CAST(JSON_VALUE(data,'$.video_watched_75_percent') AS INT64) as video_75_completion,
    SAFE_CAST(JSON_VALUE(data,'$.video_watched_100_percent') AS INT64)as video_completion,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.campaign_id') ,JSON_VALUE(data,'$.date'),JSON_VALUE(data,'$.ad_id')
    order by _sdc_extracted_at desc
    ) as row_num
    from `moe-main.reddit_raw.reports`
),filtered_data as (
   select * from initial_data where row_num = 1 
),
final as (
    select SUM(clicks) as clicks,
    SUM(impressions) as impressions,
    SUM(media_cost)/1000000 as media_cost,
    SUM(video_25_completion) as video_25_completion,
    SUM(video_50_completion) as video_50_completion,
    SUM(video_75_completion) as video_75_completion,
    SUM(video_completion) as video_completion,
    campaign_id,
    ad_id,
    date
    from filtered_data
    group by campaign_id,ad_id,date
),
joined as (
    select final.* except(ad_id,campaign_id),
    ads.*except(ad_name), ad_name as creative_name,
    campaigns.* except(campaign_id)
    from final left join ads on final.ad_id = ads.ad_id
    left join campaigns on final.campaign_id = campaigns.campaign_id
)
select * ,
CASE
    WHEN ARRAY_LENGTH(SPLIT(creative_name,'_')) <8 THEN 'Other'
    ELSE
        SPLIT(creative_name,'_')[OFFSET(7)] 
END AS audience_name,
  CASE 
    WHEN ARRAY_LENGTH(SPLIT(creative_name,'_')) <8 THEN 'Other'
    ELSE
        SPLIT(creative_name,'_')[OFFSET(6)] 
  END AS ad_format,
CASE 
    WHEN ARRAY_LENGTH(SPLIT(creative_name,'_')) <8 THEN 'Other'
    ELSE
        SPLIT(creative_name,'_')[OFFSET(5)] 
END AS creative_descr,
CASE 
    WHEN ARRAY_LENGTH(SPLIT(creative_name,'_')) <8 THEN 'Other'
    ELSE
        SPLIT(creative_name,'_')[OFFSET(7)] 
END AS ad_format_detail,
CASE
    WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) <=1 THEN 'Other'
    ELSE
        SPLIT(campaign_name,'_')[OFFSET(1)] 
END AS campaign_descr,
'Reddit' as publisher

    
    
from joined