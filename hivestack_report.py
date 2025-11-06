#!/usr/bin/env python3
"""
List all available Hivestack report definitions (report IDs) via API.

Docs:
    https://docs.hivestack.com/reference/getreportdefinitions
"""

import requests
import json
import sys

API_BASE = "https://apps.hivestack.com/api/v2"


def list_report_definitions(access_key: str, secret_key: str):
    """
    Fetch all report definitions from Hivestack API.
    
    Args:
        access_key (str): Your Hivestack API access key.
        secret_key (str): Your Hivestack API secret key.
    """
    url = f"{API_BASE}/reportdefinitions"
    headers = {
        "hs-auth": f"apikey {access_key}",
        "Accept": "application/json",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}", file=sys.stderr)
        return None

    try:
        data = response.json()
    except json.JSONDecodeError:
        print("❌ Failed to parse JSON response", file=sys.stderr)
        return None

    reports = data
    print(f"✅ Found {len(reports)} report definitions\n")

    for report in reports:
        report_id = report.get("id") or report.get("report_definition_id")
        name = report.get("name") or report.get("title")
        description = report.get("description", "")
        print(f"- ID: {report_id}\n  Name: {name}\n  Description: {description}\n")

    return reports


# Example usage
if __name__ == "__main__":
    ACCESS_KEY = "$2b$12$AvCbRGDJQIaTmaT.gGzXJ.:b22dcb8b-685b-4f15-9a75-76d9897353f6"
    SECRET_KEY = "YOUR_SECRET_KEY"
    
    list_report_definitions(ACCESS_KEY, SECRET_KEY)
