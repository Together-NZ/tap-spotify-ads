import requests

APP_ID = "7235900511461851138"              # aka client_key
APP_SECRET = "e1b5f2e5f1981bc4778e9685a14ef287d0092f26"      # required for this endpoint in most setups
ACCESS_TOKEN = "17026c8a33d3cf68f58ad4d4606d344b18430e2e"  # from your OAuth exchange

url = "https://business-api.tiktok.com/open_api/v1.3/oauth2/advertiser/get/"

headers = {
    "Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

params = {
    "app_id": APP_ID,
    "secret": APP_SECRET,
}

resp = requests.get(url, headers=headers, params=params, timeout=30)
print(resp.status_code, resp.text)

# Optional: assert your advertiser is visible
ADVERTISER_ID = "7543837460053213202"
try:
    data = resp.json()
    advertisers = (data.get("data") or {}).get("list") or []
    ids = {str(item.get("advertiser_id")) for item in advertisers}
    print("Found IDs:", ids)
    if ADVERTISER_ID in ids:
        print("✅ Token can see your advertiser.")
    else:
        print("❌ Advertiser not in token scope.")
except Exception as e:
    print("Failed to parse JSON:", e)
