"""SpotifyAds tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_spotifyads import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapSpotifyAds(Tap):
    """Singer tap for SpotifyAds."""

    name = "tap-spotifyads"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "refresh_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Refresh Token",
            description="The token to exchange for an access token",
        ),
        th.Property(
            "ad_account_ids",
            th.StringType(nullable=False),
            required=True,
            title="Ad Account IDs",
            description="Ad account IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType(nullable=True),
            description="The latest record date to sync",
        ),
        th.Property(
            "client_id",
            th.StringType(nullable=False),
            required=True,
            title="Client ID",
            description="The client ID to use for authentication",
        ),
        th.Property(
            "client_secret",
            th.StringType(nullable=False),
            required=True,
            title="Client Secret",
            description="The client secret to use for authentication",
        ),
        th.Property(
            "redirect_url",
            th.StringType(nullable=False),
            required=True,
            title="Redirect URL",
            description="The redirect URL to use for authentication",
        )
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.SpotifyAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.AdAccountStream(self),
            streams.CampaignStream(self),
            streams.AdSetsStream(self),
            streams.AdsStream(self),
            streams.AdsReportStream(self),
            streams.CampaignReportStream(self)
        ]


if __name__ == "__main__":
    TapSpotifyAds.cli()
