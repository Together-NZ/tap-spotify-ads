  {{ config(
    materialized='table',
) }}
-- Step 1: Get the latest campaign name per campaign_id
WITH new_campaigns AS (
  SELECT
    campaign_id,
    campaign_name AS new_campaign_name,
    MAX(_DATA_DATE) AS latest_date
  FROM `together-internal.google_ads_data_transfer.ads_Campaign_6544860891`
  GROUP BY campaign_id, campaign_name
),
-- Step 2: For each campaign_id, get all distinct campaign names (including old ones)
all_campaign_names AS (
  SELECT DISTINCT
    campaign_id,
    campaign_name AS old_campaign_name
  FROM `together-internal.google_ads_data_transfer.ads_Campaign_6544860891`
)

-- Step 3: Join old names to the new name per campaign_id
SELECT
  a.campaign_id,
  a.old_campaign_name,
  n.new_campaign_name
FROM all_campaign_names a
JOIN (
  SELECT campaign_id, new_campaign_name
  FROM (
    SELECT
      campaign_id,
      campaign_name AS new_campaign_name,
      _DATA_DATE,
      ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _DATA_DATE DESC) AS rn
    FROM `together-internal.google_ads_data_transfer.ads_Campaign_6544860891`
  )
  WHERE rn = 1
) n
  ON a.campaign_id = n.campaign_id
WHERE a.old_campaign_name != n.new_campaign_name