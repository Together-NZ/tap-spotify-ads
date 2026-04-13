{{ config(
    materialized='table',
) }}

{{ google_ads.google_ads_demand(client_id=env_var('GOOGLE_ADS_CLIENT_ID', '8392440135')) }}