  {{ config(
    materialized='incremental',
) }}

WITH reference AS (
    SELECT * FROM `together-internal.google_ads_data_transfer.ads_GeoStats_6544860891`
),
geo_location AS (
    SELECT * FROM `together-internal.google_ads_geo_target_location_macthing.geoTargetConstant_location`
)
select ref.* from reference as ref left join geo_location as geo on geo.geoTargetConstant.resourceName = ref.segments_geo_target_most_specific_location

{% if is_incremental() %}

where _DATA_DATE > (select max(_DATA_DATE) from {{ this }})

{% endif %}