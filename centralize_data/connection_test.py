import requests

# Replace this with your valid LinkedIn Marketing API access token
ACCESS_TOKEN = "AQXPzQF48o4mZ_W05E-21QtWuKIf3Es0haMi-QKRj_ktzaBFW1Gy5mHdRcSYCtwwNLqis_8E4lNSW3zPd70NWIAA3tqRPdJxa1PNnfcH4lXhZYVi6M7ZiMx-Bq03uye0RA8JdM5MvlqNUIVRDUPCMCa-0aEU4z-YPRsYPgU5xuvg2MU0tjvwU4l5WsPWPuSW_3_cVrSMx5wKuZtTxwR4LQRDihvCVx3G0fWfT_A_Kng7-8d6J7gSeh6_IO5HnmmYJo9tf1gbqXiVVrJ1efF9zfefPLa7S32RLolI7X7znipyey38IXB1rlGhwBOnnl9FmT9MZqsYS7JcZgykWi8BnA7j_2JTdQ"

url = "https://api.linkedin.com/rest/adAccounts/503820360/creatives/urn%3Ali%3AsponsoredCreative%3A889256646"
headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "LinkedIn-Version": "202507",            # use the latest supported version
    "X-Restli-Protocol-Version": "2.0.0",
    "Accept": "application/json"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print("❌ Request failed")
    print("Status code:", response.status_code)
    print("Response:", response.text)
