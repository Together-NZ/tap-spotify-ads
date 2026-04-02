{{ config(
    materialized='table',
) }}
WITH final_result AS (
  {{ dash_table_general_process.dash_union_non_search(source_name='dash_union', table_name='dash_table',sub_brands=env_var('SUB_BRANDS', 'null')) }}
 )
SELECT
  COALESCE(t2.present, t1.publisher) AS publisher,
  t1.*
EXCEPT(publisher)
FROM final_result AS t1
LEFT JOIN `together-internal.publisher_naming.publisher_naming` AS t2
  ON LOWER(t1.publisher) = LOWER(t2.publisher)
