{{ config(
    materialized='table',
) }}

{{ google_ads.google_ads_search(client_id=9089059941) }}
