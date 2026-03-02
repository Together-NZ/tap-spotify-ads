"""Stream type classes for tap-spotifyads."""

from __future__ import annotations

from typing import Any

from singer_sdk import typing as th

from tap_spotifyads.client import SpotifyAdsStream
import pathlib
from datetime import date,timedelta,datetime

SCHEMAS_DIR = pathlib.Path(__file__).resolve().parent.parent / "schema"

REPORT_FIELDS = [
    "CLICKS",
    "FIRST_QUARTILES",
    "IMPRESSIONS",
    "MIDPOINTS",
    "SPEND",
    "REACH",
    "VIDEO_VIEWS",
    "THIRD_QUARTILES",
    "LEADS",
    "STARTS",
    "PURCHASES",
    "REVENUE",
    "PAGE_VIEWS",
    "ADD_TO_CART",
    "AVERAGE_ORDER_VALUE",
    "RETURN_ON_AD_SPEND",
    "CUSTOMER_ACQUISITION_COST",
    "COST_PER_LEAD",
    "START_CHECKOUT",
    "SIGN_UPS",
    "PRODUCTS",
]


class AdAccountStream(SpotifyAdsStream):
    """Ad account metadata stream."""

    path = ""
    name = "ad_accounts"
    primary_keys = ("id",)
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "ad_account.json"


class CampaignStream(SpotifyAdsStream):
    """Campaigns stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.campaigns[*]"
    schema_filepath = SCHEMAS_DIR / "campaign.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {"campaign_id": record["id"]}


class AdSetsStream(SpotifyAdsStream):
    """Ad sets stream."""

    name = "ad_sets"
    path = "/ad_sets"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.ad_sets[*]"
    schema_filepath = SCHEMAS_DIR / "adsets.json"


class AdsStream(SpotifyAdsStream):
    """Ads stream."""

    name = "ads"
    path = "/ads"
    primary_keys = ("id",)
    replication_key = None
    records_jsonpath = "$.ads[*]"
    schema_filepath = SCHEMAS_DIR / "ads.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {"ad_id": record["id"]}


class AdsReportStream(SpotifyAdsStream):
    """Daily report per ad."""

    records_jsonpath = "$.rows[*]"
    name = "ads_daily_report"
    path = "/aggregate_reports"
    primary_keys = ("entity_id", "start_time", "end_time")
    replication_key = None
    parent_stream_type = AdsStream
    schema_filepath = SCHEMAS_DIR / "insight_daily_report.json"

    def get_url_params(
        self, context: dict | None, next_page_token: Any | None
    ) -> list[tuple[str, str]]:
        ad_id = context["ad_id"] if context else ""
        start_date = datetime.strptime(self.config.get("start_date", "2025-01-01"), "%Y-%m-%d").date()
        end_date = datetime.strptime(self.config.get("end_date", "2025-12-31"), "%Y-%m-%d").date()
        params = [
            ("entity_ids_type", "AD"),
            ("entity_type", "AD"),
            ("report_start", f"{start_date}T00:00:00Z"),
            ("report_end", f"{end_date}T00:00:00Z"),
            ("granularity", "DAY"),
            ("entity_ids", ad_id),
        ]
        for f in REPORT_FIELDS:
                    params.append(("fields", f))
        if next_page_token:
                params.append(("continuation_token", str(next_page_token)))
        return params


class CampaignReportStream(SpotifyAdsStream):
    """Daily report per campaign."""

    records_jsonpath = "$.rows[*]"
    name = "campaign_daily_report"
    path = "/aggregate_reports"
    primary_keys = ("entity_id", "start_time", "end_time")
    replication_key = None
    parent_stream_type = CampaignStream
    schema_filepath = SCHEMAS_DIR / "insight_daily_report.json"

    def get_url_params(
        self, context: dict | None, next_page_token: Any | None
    ) -> list[tuple[str, str]]:
        campaign_id = context["campaign_id"] if context else ""
        start_date = datetime.strptime(self.config.get("start_date", "2025-01-01"), "%Y-%m-%d").date()
        end_date = datetime.strptime(self.config.get("end_date", "2025-12-31"), "%Y-%m-%d").date()
 
        params=[
                ("entity_ids_type", "CAMPAIGN"),
                ("entity_type", "CAMPAIGN"),
                ("report_start", f"{start_date}T00:00:00Z"),
            ("report_end", f"{end_date}T00:00:00Z"),
            ("granularity", "DAY"),
            ("entity_ids", campaign_id),
        ]
        for f in REPORT_FIELDS:
            params.append(("fields", f))
        if next_page_token:
            params.append(("continuation_token", str(next_page_token)))
        return params
