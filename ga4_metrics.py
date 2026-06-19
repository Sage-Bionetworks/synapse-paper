"""Pull yearly active users across Sage-owned domains from GA4.

Produces the figure used in the Synapse paper, e.g. "web traffic logs from 2024
indicate N active users": GA4's activeUsers metric (a user with an engaged
interaction) counted once across all Sage-owned domains for a calendar year.
"""

import argparse
import json
import re

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    FilterExpressionList,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account


# GA4 Property ID from https://analytics.google.com/analytics/web/#/a29804340p311611973
PROPERTY_ID = "311611973"


# Sage-owned apex domains (suffix match). Anything not matching is dropped as
# spoofed/scraped GA4 hits. Add new apexes here when Sage stands up new portals.
ALLOW_HOST_PATTERN = re.compile(
    "|".join([
        r"(^|\.)synapse\.org$",
        # r"(^|\.)sagebionetworks\.org$",
        # # r"(^|\.)sageit\.org$",
        # r"(^|\.)adknowledgeportal\.org$",
        # r"(^|\.)ampadportal\.org$",
        # r"(^|\.)nfdataportal\.org$",
        # r"(^|\.)eliteportal\.org$",
    ])
)

# Pre-prod subdomains that DO match an allowed apex (e.g. staging.synapse.org)
# but shouldn't pollute production analytics.
PREPROD_HOST_PATTERN = re.compile(
    r"^(staging|tst|dev|staging-signin|cdn-www|dev-signin|portal-dev\.dev)\."
)


def get_client(credentials_path: str) -> BetaAnalyticsDataClient:
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def _allowed_host_filter() -> FilterExpression:
    """GA4 dimension filter: hostName is in the allowlist and not a pre-prod variant."""
    return FilterExpression(
        and_group=FilterExpressionList(
            expressions=[
                FilterExpression(filter=Filter(
                    field_name="hostName",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.PARTIAL_REGEXP,
                        value=ALLOW_HOST_PATTERN.pattern,
                    ),
                )),
                FilterExpression(not_expression=FilterExpression(filter=Filter(
                    field_name="hostName",
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.PARTIAL_REGEXP,
                        value=PREPROD_HOST_PATTERN.pattern,
                    ),
                ))),
            ]
        )
    )


def get_yearly_active_users(
    credentials_path: str,
    year: int,
    property_id: str = PROPERTY_ID,
) -> dict:
    """Deduplicated active users across all Sage-owned domains for a calendar year.

    GA4's activeUsers metric (a user with an engaged interaction) counted once
    across all allowed domains, not the sum of per-domain rows: a user who hit
    both synapse.org and accounts.synapse.org is counted once. Spoofed/scraped
    hostnames and pre-prod variants are excluded via the allowlist.

    Args:
        credentials_path: Path to GA4 service account JSON file.
        year: Four-digit calendar year (e.g. 2024).
        property_id: GA4 numeric property ID.

    Returns:
        Dict with the year, date range queried, and active_users count.
    """
    start_date = f"{year:04d}-01-01"
    end_date = f"{year:04d}-12-31"
    client = get_client(credentials_path)
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        metrics=[Metric(name="activeUsers")],
        dimension_filter=_allowed_host_filter(),
    )
    response = client.run_report(request)
    active = int(response.rows[0].metric_values[0].value) if response.rows else 0
    return {
        "property_id": property_id,
        "year": year,
        "start_date": start_date,
        "end_date": end_date,
        "active_users": active,
    }


def get_yearly_country_distribution(
    credentials_path: str,
    year: int,
    property_id: str = PROPERTY_ID,
) -> dict:
    """Active users by country across Sage-owned domains for a calendar year.

    Produces the geographic-distribution figures, e.g. "geographic analysis of
    user locations in 2025 (n=N) shows usage is centered in the United States
    (88.48%)". activeUsers is broken down by GA4's `country` dimension under the
    same allowlist as get_yearly_active_users, then each country's share is its
    activeUsers over the summed total (n). Because a user active from more than
    one country is counted in each, n is the sum of per-country rows and may
    slightly exceed the deduplicated yearly active-user count.

    Args:
        credentials_path: Path to GA4 service account JSON file.
        year: Four-digit calendar year (e.g. 2025).
        property_id: GA4 numeric property ID.

    Returns:
        Dict with the year, date range, total n, and a `countries` list of
        {country, active_users, pct} sorted by active_users descending.
    """
    start_date = f"{year:04d}-01-01"
    end_date = f"{year:04d}-12-31"
    client = get_client(credentials_path)
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="country")],
        metrics=[Metric(name="activeUsers")],
        dimension_filter=_allowed_host_filter(),
    )
    response = client.run_report(request)

    countries = sorted(
        (
            {
                "country": row.dimension_values[0].value,
                "active_users": int(row.metric_values[0].value),
            }
            for row in response.rows
        ),
        key=lambda r: r["active_users"],
        reverse=True,
    )
    total = sum(c["active_users"] for c in countries)
    for c in countries:
        c["pct"] = round(100 * c["active_users"] / total, 2) if total else 0.0

    return {
        "property_id": property_id,
        "year": year,
        "start_date": start_date,
        "end_date": end_date,
        "n": total,
        "countries": countries,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull deduplicated yearly active users across Sage domains from GA4"
    )
    parser.add_argument("credentials", help="Path to service account JSON file")
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year to report (e.g. --year 2024)",
    )
    parser.add_argument(
        "--countries",
        action="store_true",
        help="Report active users broken down by country (geographic distribution)",
    )
    parser.add_argument("--property-id", default=PROPERTY_ID)
    args = parser.parse_args()
    result = get_yearly_active_users(
        args.credentials, args.year, args.property_id
    )
    if args.countries:
        result = get_yearly_country_distribution(
            args.credentials, args.year, args.property_id
        )
       
    print(json.dumps(result, indent=2))
