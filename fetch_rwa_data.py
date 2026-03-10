#!/usr/bin/env python3
"""
Fetches RWA holder data from api.rwa.xyz and saves it as a CSV snapshot.
Designed to run via GitHub Actions to build a historical database.
Uploads data to Dune Analytics.
"""

import csv
import json
import os
import sys
import time
from urllib.request import urlopen, Request
from urllib.parse import urlencode

# Load .env file if present (for local development)
_env_file = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_file):
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

API_URL = "https://api.rwa.xyz/v4/tokens/aggregates/timeseries"
API_TOKEN = os.environ.get("RWA_API_TOKEN", "")
SNAPSHOTS_DIR = "snapshots"
DAILY_SNAPSHOT_FILE = os.path.join(SNAPSHOTS_DIR, "rwa_holders_daily.csv")
DUNE_TABLE_NAME = "rwa_holders_daily"
DUNE_NAMESPACE = "plume"

# Target tokens: id -> display name
TARGET_TOKENS = {
    3699: "Plume nELIXIR",
    3700: "Plume nINSTO",
    3701: "Plume nPAYFI",
    3702: "Plume nCREDIT",
    3703: "Plume nALPHA",
    3704: "Plume nETF",
    3705: "Plume nBASIS",
    3706: "Plume nTBILL",
    3759: "Ethereum nELIXIR",
    5094: "Plume XAUm",
    4453: "Plume Midas mBASIS",
    9466: "Solana nTBILL",
    9467: "Solana nBASIS",
    9468: "Solana nALPHA",
}


def fetch_page(query, timeout=90):
    """Fetch a single page from the RWA API."""
    params = urlencode({"query": json.dumps(query)})
    url = f"{API_URL}?{params}"
    req = Request(url, headers={
        "Authorization": f"Bearer {API_TOKEN}",
        "User-Agent": "Mozilla/5.0",
    })
    with urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_api_data(retries=3, timeout=90):
    """Fetch all pages from the RWA API and return data in tokenTimeseries format."""
    base_query = {
        "filter": {
            "operator": "and",
            "filters": [
                {"field": "measure_id", "operator": "equals", "value": 15},
                {"field": "date", "operator": "onOrAfter", "value": "2025-06-01T00:00:00.000Z"},
            ],
        },
        "aggregate": {
            "groupBy": "token",
            "aggregateFunction": "sum",
            "interval": "day",
        },
        "sort": {"direction": "asc", "field": "date"},
        "pagination": {"page": 1, "perPage": 200},
    }

    all_results = []
    page = 1

    while True:
        query = {**base_query, "pagination": {"page": page, "perPage": 200}}
        for attempt in range(1, retries + 1):
            try:
                print(f"Fetching page {page}, attempt {attempt}/{retries}...")
                data = fetch_page(query, timeout=timeout)
                break
            except Exception as e:
                print(f"Error on page {page}, attempt {attempt}: {e}")
                if attempt < retries:
                    time.sleep(10)
                else:
                    raise

        all_results.extend(data.get("results", []))
        pagination = data.get("pagination", {})
        page_count = pagination.get("pageCount", 1)
        print(f"  Page {page}/{page_count}, got {len(data.get('results', []))} tokens")
        if page >= page_count:
            break
        page += 1

    # Transform to tokenTimeseries format, filtering to target tokens only
    token_timeseries = {}
    for item in all_results:
        group = item.get("group", {})
        token_id = group.get("id")
        if token_id not in TARGET_TOKENS:
            continue
        display_name = TARGET_TOKENS[token_id]
        timeseries = [
            {"date": point[0], "holders": point[1]}
            for point in item.get("points", [])
        ]
        token_timeseries[str(token_id)] = {
            "id": token_id,
            "name": display_name,
            "timeseries": timeseries,
        }

    return {
        "measure": "holding_addresses_count",
        "tokenTimeseries": token_timeseries,
    }


def load_existing_data():
    """Load existing CSV data if it exists."""
    existing_data = {}
    if os.path.exists(DAILY_SNAPSHOT_FILE):
        with open(DAILY_SNAPSHOT_FILE, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                date = row["date"]
                existing_data[date] = {k: v for k, v in row.items() if k != "date"}
    return existing_data


def merge_data(existing_data, api_data):
    """Merge existing data with new API data."""
    # Get all token names from API
    token_names = set()
    for token_id, token_data in api_data.get("tokenTimeseries", {}).items():
        token_names.add(token_data["name"])

    # Also include any existing token names
    if existing_data:
        first_row = next(iter(existing_data.values()))
        token_names.update(first_row.keys())

    token_names = sorted(token_names)

    # Build merged dataset
    merged = {}

    # Add existing data
    for date, values in existing_data.items():
        merged[date] = {name: values.get(name, "") for name in token_names}

    # Add/update from API data
    for token_id, token_data in api_data.get("tokenTimeseries", {}).items():
        token_name = token_data["name"]
        for entry in token_data.get("timeseries", []):
            date = entry["date"]
            holders = entry["holders"]

            if date not in merged:
                merged[date] = {name: "" for name in token_names}

            merged[date][token_name] = str(holders)

    return merged, token_names


def save_csv(data, token_names):
    """Save merged data to CSV file."""
    os.makedirs(SNAPSHOTS_DIR, exist_ok=True)

    # Sort dates chronologically
    sorted_dates = sorted(data.keys())

    with open(DAILY_SNAPSHOT_FILE, "w", newline="") as f:
        fieldnames = ["date"] + sorted(token_names)
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for date in sorted_dates:
            row = {"date": date}
            row.update(data[date])
            writer.writerow(row)

    print(f"Saved {len(sorted_dates)} rows to {DAILY_SNAPSHOT_FILE}")


def main():
    if not API_TOKEN:
        print("RWA_API_TOKEN environment variable is not set")
        sys.exit(1)

    print(f"Fetching data from {API_URL}...")
    try:
        api_data = fetch_api_data()
    except Exception as e:
        print(f"API unavailable after all retries: {e}")
        print("Skipping update - will retry at next scheduled run")
        return

    print(f"Found {len(api_data.get('tokenTimeseries', {}))} tokens")

    print("Loading existing data...")
    existing_data = load_existing_data()
    print(f"Existing records: {len(existing_data)}")

    print("Merging data...")
    merged_data, token_names = merge_data(existing_data, api_data)
    print(f"Total records after merge: {len(merged_data)}")

    print("Saving CSV...")
    save_csv(merged_data, token_names)

    # Upload to Dune if API key is available
    dune_api_key = os.environ.get("DUNE_API_KEY")
    if dune_api_key:
        upload_to_dune(dune_api_key)
    else:
        print("DUNE_API_KEY not set, skipping Dune upload")

    print("Done!")


def upload_to_dune(api_key):
    """Upload CSV data to Dune Analytics."""
    try:
        from dune_client.client import DuneClient

        print("Uploading to Dune Analytics...")
        dune = DuneClient(api_key)

        with open(DAILY_SNAPSHOT_FILE) as f:
            data = f.read()

        table = dune.upload_csv(
            data=data,
            description="RWA token holder counts from Nest protocol, updated every 12 hours",
            table_name=DUNE_TABLE_NAME,
            is_private=False,
        )

        print(f"Successfully uploaded to Dune: {DUNE_NAMESPACE}.dataset_{DUNE_TABLE_NAME}")
        return table

    except ImportError:
        print("dune-client not installed, skipping Dune upload")
    except Exception as e:
        print(f"Error uploading to Dune: {e}")
        raise


if __name__ == "__main__":
    main()
