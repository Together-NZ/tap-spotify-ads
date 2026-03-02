import base64
import http.client
import json
import time
import urllib.parse


class SpotifyAuth:
    TOKEN_URL_HOST = "accounts.spotify.com"
    TOKEN_URL_PATH = "/api/token"

    def __init__(self, client_id, client_secret, redirect_url, refresh_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_url = redirect_url
        self.refresh_token = refresh_token
        self.access_token = None
        self.access_token_expires_at = 0

    def get_access_token(self):
        if self.access_token is None or self.access_token_expires_at < time.time():
            self.refresh_access_token()
        return self.access_token

    def refresh_access_token(self):
        credentials = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("ascii")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {credentials}",
        }

        payload = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        })

        conn = http.client.HTTPSConnection(self.TOKEN_URL_HOST)
        conn.request("POST", self.TOKEN_URL_PATH, payload, headers)
        response = conn.getresponse()
        data = json.loads(response.read().decode("utf-8"))
        conn.close()

        if "error" in data:
            raise RuntimeError(
                f"Spotify token refresh failed: {data.get('error_description', data['error'])}"
            )

        self.access_token = data["access_token"]
        self.access_token_expires_at = time.time() + data.get("expires_in", 3600) - 60
