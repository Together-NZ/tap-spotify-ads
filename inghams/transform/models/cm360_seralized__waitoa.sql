{{ config(
    materialized='table',
) }}
WITH cm360reference AS (
    -- cm360 seralized dataset
    SELECT
        JSON_VALUE(JSON_EXTRACT(data, "$.placementId")) AS placement_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.advertiser")) AS advertiser,
        JSON_VALUE(JSON_EXTRACT(data, "$.creativeType")) AS creative_type,
        JSON_VALUE(JSON_EXTRACT(data, "$.creativeId")) AS creative_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.creative")) AS creative_name,
        JSON_VALUE(JSON_EXTRACT(data, "$.advertiserId")) AS advertiser_id,
        SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.campaignEndDate"))) AS campaign_end_date,
        JSON_VALUE(JSON_EXTRACT(data, "$.campaignId")) AS campaign_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.campaign")) AS campaign_name,
        SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.campaignStartDate"))) AS campaign_start_date,
        JSON_VALUE(JSON_EXTRACT(data, "$.clickThroughUrl")) AS click_through_url,
        SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.date"))) AS date,
        JSON_VALUE(JSON_EXTRACT(data, "$.placementCostStructure")) AS placement_cost_structure,
        SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.placementEndDate"))) AS placement_end_date,
        JSON_VALUE(JSON_EXTRACT(data, "$.placement")) AS placement,
        JSON_VALUE(JSON_EXTRACT(data, "$.packageRoadblockId")) AS package_roadblock_id,
        JSON_VALUE(JSON_EXTRACT(data, "$.packageRoadblock")) AS package_roadblock,
        JSON_VALUE(JSON_EXTRACT(data, "$.placementSize")) AS placement_size,
        SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.placementStartDate"))) AS placement_start_date,
        JSON_VALUE(JSON_EXTRACT(data, "$.placementStrategy")) AS placement_strategy,
        JSON_VALUE(JSON_EXTRACT(data, "$.siteKeyname")) AS site_keyname,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.clicks")) AS INT64) AS clicks,
        CAST(JSON_VALUE(JSON_EXTRACT(data, "$.impressions")) AS INT64) AS impressions,
        ROW_NUMBER() OVER (
            PARTITION BY 
                JSON_VALUE(JSON_EXTRACT(data, "$.placementId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.advertiser")),
                JSON_VALUE(JSON_EXTRACT(data, "$.creativeType")),
                JSON_VALUE(JSON_EXTRACT(data, "$.creativeId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.creative")),
                JSON_VALUE(JSON_EXTRACT(data, "$.advertiserId")),
                SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.campaignEndDate"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.campaignId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.campaign")),
                SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.campaignStartDate"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.clickThroughUrl")),
                SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.date"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.placementCostStructure")),
                SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.placementEndDate"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.placement")),
                JSON_VALUE(JSON_EXTRACT(data, "$.packageRoadblockId")),
                JSON_VALUE(JSON_EXTRACT(data, "$.packageRoadblock")),
                JSON_VALUE(JSON_EXTRACT(data, "$.placementSize")),
                SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.placementStartDate"))),
                JSON_VALUE(JSON_EXTRACT(data, "$.placementStrategy")),
                JSON_VALUE(JSON_EXTRACT(data, "$.siteKeyname"))
            ORDER BY 
                SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(JSON_EXTRACT(data, "$.date"))) DESC
        ) AS row_num
    FROM 
        `together-internal.cm360_raw.cm360_report_stream`
    WHERE 
        LOWER(JSON_VALUE(JSON_EXTRACT(data, "$.advertiser"))) = 'waitoa'
)    
select * from cm360reference where  row_num = 1
{% if is_incremental() %}
  -- Only include new or updated rows
  AND date >= (SELECT MAX(date) FROM {{ this }})
{% endif %}