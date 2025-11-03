{{ config(
    materialized='view',
) }}
with reference as (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY`Advertiser_Offline`,Product,Channel,Date,`Sub_Category`,Category,Brand,Date,report_date ORDER BY report_date DESC) AS row_num
  FROM `arvida-main.neilson_raw.neilson_staging`
)
select * from reference 