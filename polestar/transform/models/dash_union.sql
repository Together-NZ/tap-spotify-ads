{{ config(
    materialized='table',
) }}
WITH final_result AS (
  {{ dash_table_general_process.dash_union_non_search(source_name='dash_union', table_name='dash_table',sub_brands=env_var('SUB_BRANDS', 'null')) }}
   UNION ALL 
   {{ dash_table_general_process.dash_union_search(source_name='dash_union_search', table_name='dash_table_search',sub_brands=env_var('SUB_BRANDS', 'null')) }}
 )
SELECT
  -- Replace publisher from table2 if matched, else keep original
  COALESCE(t2.present, t1.publisher) AS publisher,
  t1.*
EXCEPT(publisher) -- exclude original publisher to avoid duplicate columns

FROM final_result AS t1
LEFT JOIN `together-internal.publisher_naming.publisher_naming` AS t2
  ON LOWER(t1.publisher) = LOWER(t2.publisher)