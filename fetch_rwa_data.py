#!/usr/bin/env python3
"""
Fetches RWA legacy data from the API and saves it as a CSV snapshot.
Designed to run via GitHub Actions to build a historical database.
Uploads data to Dune Analytics.
"""

import csv
import json
import os
import time
from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError

API_URL = "https://rwa-api-production.up.railway.app/rwalegacy"
SNAPSHOTS_DIR = "snapshots"
DAILY_SNAPSHOT_FILE = os.path.join(SNAPSHOTS_DIR, "rwa_holders_daily.csv")
DUNE_TABLE_NAME = "rwa_holders_daily"
DUNE_NAMESPACE = "plume"


def fetch_api_data(retries=3, timeout=90):
    """Fetch data from the RWA API with retries."""
    for attempt in range(1, retries + 1):
        try:
            print(f"Attempt {attempt}/{retries}...")
            with urlopen(API_URL, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as e:
            print(f"Error on attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(10)
            else:
                raise


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
    print(f"Fetching data from {API_URL}...")
    try:
        api_data = fetch_api_data()
    except Exception as e:
        print(f"API unavailable after all retries: {e}")
        print("Skipping update - will retry at next scheduled run")
        return

    print(f"Found {api_data.get('tokenCount', 0)} tokens")

    print("Loading existing data...")
    existing_data = load_existing_data()
    print(f"Existing records: {len(existing_data)}")

    print("Merging data...")
    merged_data, token_names = merge_data(existing_data, api_data)
    print(f"Total records after merge: {len(merged_data)}")

    print("Saving CSV...")
    save_csv(merged_data, token_names)

    # Also save a timestamped raw JSON snapshot for debugging
    timestamp = datetime.utcnow().strftime("%Y-%m-%d")
    raw_file = os.path.join(SNAPSHOTS_DIR, f"raw_{timestamp}.json")
    with open(raw_file, "w") as f:
        json.dump(api_data, f, indent=2)
    print(f"Saved raw snapshot to {raw_file}")

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
