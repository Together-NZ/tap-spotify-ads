import requests
from urllib.parse import urlparse, parse_qs
from google.cloud import bigquery
client = bigquery.Client()
class Parsed_campaign_creative_click_url_name:
    def __init__(self):
        self.access_token=None
        raise NotImplementedError
    @classmethod
    def initialise_access_token(self,access_token):
        self.access_token = access_token
    @classmethod
    def get_creative_data(self,account_id:str, creative_id: str):
        """Fetch LinkedIn creative and extract campaign name, creative name, and click URL"""
        try:
            url = f"https://api.linkedin.com/rest/adAccounts/{account_id}/creatives/urn%3Ali%3AsponsoredCreative%3A{creative_id}"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "X-RestLi-Protocol-Version": "2.0.0",
                "LinkedIn-Version": "202507",
                "Accept": "application/json",
            }

            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            # --- Debug: uncomment if you want to inspect the raw response ---
            # import json; print(json.dumps(data, indent=2))

            # Try to get creative name and click URL from possible paths
            creative_name = None
            click_url = None

            # SponsoredUpdate creatives often have this structure
            variables = data.get("variables", {}).get("data", {}).get("com.linkedin.ads.SponsoredUpdateCreativeVariables", {})
            if variables:
                creative_name = variables.get("share~", {}).get("subject")
                content_entities = variables.get("share~", {}).get("content", {}).get("contentEntities", [])
                if isinstance(content_entities, list) and content_entities:
                    click_url = content_entities[0].get("entityLocation")

            # Fallbacks
            if not creative_name:
                creative_name = data.get("name") or data.get("title")
            if not click_url:
                click_url = data.get("landingPageUrl") or data.get("clickUrl")

            # Extract utm_campaign from URL if available
            campaign_name = None
            if click_url:
                parsed = urlparse(click_url)
                qs = parse_qs(parsed.query)
                campaign_name = qs.get("utm_campaign", [None])[0]

            return campaign_name, creative_name, click_url

        except requests.HTTPError as e:
            print(f"❌ HTTP error: {e.response.status_code}")
            print(e.response.text)
        except Exception as e:
            print(f"❌ Error: {e}")

        return None, None, None
