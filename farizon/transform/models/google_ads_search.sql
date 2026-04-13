{{ config(
    materialized='table',
) }}

{{ google_ads.google_ads_search(client_id=8392440135) }}
