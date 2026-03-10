#!/usr/bin/env python3
"""
Fetches per-protocol token data from api.rwa.xyz, normalizes it,
diffs against yesterday's snapshot, and produces tiny daily summaries
for AI agent reading.

Pipelines run independently per protocol: ondo, nest, securitize, centrifuge

Output structure:
  data/raw/<protocol>/<YYYY-MM-DD>.json
  data/brwa-snapshot/<protocol>/<YYYY-MM-DD>.snapshot.json
  data/summaries/<protocol>/<YYYY-MM-DD>.summary.json
"""

import io
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
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

API_URL = "https://api.rwa.xyz/v4/tokens"
API_TOKEN = os.environ.get("RWA_API_TOKEN", "")

DRIVE_ROOT_FOLDER = "1xIc4KA28H3ETExDym-fK9EARWTCbxvuP"
GOOGLE_SA_JSON    = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

PROTOCOLS = ["ondo", "nest", "securitize", "centrifuge"]

CORE_METRICS = [
    "total_asset_value_dollar",
    "total_supply_token",
    "holding_addresses_count",
    "trailing_7_day_active_addresses_count",
    "trailing_30_day_active_addresses_count",
    "trailing_7_day_transfer_count",
    "trailing_30_day_transfer_count",
    "trailing_7_day_transfer_volume",
    "trailing_30_day_transfer_volume",
    "top_holder_concentration",
    "top_5_holder_concentration",
    "top_10_holder_concentration",
]

# Metrics used for classification -> (key_if_up, key_if_down)
SIGNAL_METRICS = {
    "total_asset_value_dollar":              ("value_up",    "value_down"),
    "total_supply_token":                    ("supply_up",   "supply_down"),
    "holding_addresses_count":               ("holders_up",  "holders_down"),
    "trailing_7_day_active_addresses_count": ("activity_up", "activity_down"),
    "trailing_7_day_transfer_volume":        ("flow_up",     "flow_down"),
}

CHANGE_THRESHOLD = 0.01   # ignore changes smaller than 1%
MAX_PER_CATEGORY = 3      # max entities per category in summary
MAX_TOTAL = 7             # hard cap on total entities in summary


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_protocol_tokens(protocol_slug, retries=3, timeout=90):
    query = {
        "filter": {
            "operator": "equals",
            "field": "protocol_slug",
            "value": protocol_slug,
        },
        "sort": {"field": "market_value_dollar", "direction": "desc"},
    }
    params = urlencode({"query": json.dumps(query)})
    url = f"{API_URL}?{params}"
    req = Request(url, headers={
        "Authorization": f"Bearer {API_TOKEN}",
        "User-Agent": "Mozilla/5.0",
    })
    for attempt in range(1, retries + 1):
        try:
            with urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as e:
            print(f"  [{protocol_slug}] attempt {attempt} error: {e}")
            if attempt < retries:
                time.sleep(10)
            else:
                raise


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def _get(token, field):
    """Extract current numeric value for a metric field.
    Metrics are dicts like {val: X, val_7d: Y, ...} — we want val.
    """
    raw = token.get(field)
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw.get("val")
    return raw


def normalize_token(token):
    asset  = token.get("asset") or {}
    network = token.get("network") or {}
    asset_slug   = asset.get("slug")   or token.get("asset_slug")   or ""
    network_slug = network.get("slug") or token.get("network_slug") or ""
    token_id = f"{asset_slug}-{network_slug}" if asset_slug else str(token.get("id", "unknown"))

    result = {
        "id": token_id,
        "name": token.get("name", token_id),
        "network": network_slug,
    }
    for metric in CORE_METRICS:
        val = _get(token, metric)
        result[metric] = float(val) if val is not None else None
    return result


def normalize_snapshot(raw_data):
    tokens = raw_data.get("results", []) if isinstance(raw_data, dict) else raw_data
    snapshot = {}
    for token in tokens:
        n = normalize_token(token)
        snapshot[n["id"]] = n
    return snapshot


# ---------------------------------------------------------------------------
# Diff + classify
# ---------------------------------------------------------------------------

def _pct_change(old, new):
    if old is None or new is None:
        return None
    try:
        old, new = float(old), float(new)
    except (TypeError, ValueError):
        return None
    if old == 0:
        return None if new == 0 else 1.0   # treat 0→nonzero as 100% up
    return (new - old) / abs(old)


def classify(token_id, today, yesterday):
    """
    Returns (category, keys, magnitude) or (None, [], 0).
    category: 'inflates' | 'deflates' | 'new'
    keys: short change labels
    magnitude: sum of abs % changes (for ranking)
    """
    if yesterday is None:
        return "new", ["first_seen"], float("inf")

    old_supply = yesterday.get("total_supply_token")
    new_supply = today.get("total_supply_token")
    if (not old_supply) and new_supply and new_supply > 0:
        return "new", ["first_mint"], float("inf")

    signals = []
    for metric, (key_up, key_down) in SIGNAL_METRICS.items():
        change = _pct_change(yesterday.get(metric), today.get(metric))
        if change is None or abs(change) < CHANGE_THRESHOLD:
            continue
        key = key_up if change > 0 else key_down
        signals.append((abs(change), key))

    if not signals:
        return None, [], 0

    signals.sort(reverse=True)
    top_keys = [k for _, k in signals[:3]]
    magnitude = sum(m for m, _ in signals)

    up_count   = sum(1 for _, k in signals if k.endswith("_up"))
    down_count = sum(1 for _, k in signals if k.endswith("_down"))
    category = "inflates" if up_count >= down_count else "deflates"
    return category, top_keys, magnitude


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def produce_summary(date_str, protocol, today_snapshot, yesterday_snapshot):
    candidates = []
    for token_id, today_token in today_snapshot.items():
        yesterday_token = yesterday_snapshot.get(token_id)
        category, keys, magnitude = classify(token_id, today_token, yesterday_token)
        if category is None:
            continue
        candidates.append((magnitude, category, token_id, keys))

    candidates.sort(reverse=True)

    result = {"inflates": [], "deflates": [], "new": []}
    counts = {"inflates": 0, "deflates": 0, "new": 0}
    total = 0
    for magnitude, category, token_id, keys in candidates:
        if total >= MAX_TOTAL:
            break
        if counts[category] >= MAX_PER_CATEGORY:
            continue
        result[category].append({"id": token_id, "k": keys})
        counts[category] += 1
        total += 1

    return {
        "date": date_str,
        "protocol": protocol,
        "inflates": result["inflates"],
        "deflates": result["deflates"],
        "new": result["new"],
    }


# ---------------------------------------------------------------------------
# Google Drive upload
# ---------------------------------------------------------------------------

def _drive_service():
    """Build and return an authenticated Drive v3 service."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    sa_info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _find_or_create_folder(service, name, parent_id):
    """Return the Drive folder ID for name inside parent, creating it if needed."""
    q = (
        f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
        f"and '{parent_id}' in parents and trashed=false"
    )
    results = service.files().list(q=q, fields="files(id)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(body=meta, fields="id").execute()
    return folder["id"]


def _upsert_file(service, filename, content_bytes, parent_id):
    """Upload or overwrite filename inside parent folder."""
    from googleapiclient.http import MediaIoBaseUpload

    q = f"name='{filename}' and '{parent_id}' in parents and trashed=false"
    results = service.files().list(q=q, fields="files(id)").execute()
    files = results.get("files", [])

    media = MediaIoBaseUpload(
        io.BytesIO(content_bytes), mimetype="application/json", resumable=False
    )
    if files:
        service.files().update(fileId=files[0]["id"], media_body=media).execute()
    else:
        meta = {"name": filename, "parents": [parent_id]}
        service.files().create(body=meta, media_body=media, fields="id").execute()


def upload_summary_to_drive(protocol, date_str, summary_path):
    """Upload summary JSON to Drive: <root>/<protocol>/<date>.summary.json"""
    if not GOOGLE_SA_JSON:
        print(f"[{protocol}] GOOGLE_SERVICE_ACCOUNT_JSON not set — skipping Drive upload")
        return
    try:
        service   = _drive_service()
        folder_id = _find_or_create_folder(service, protocol, DRIVE_ROOT_FOLDER)
        filename  = f"{date_str}.summary.json"
        with open(summary_path, "rb") as f:
            content = f.read()
        _upsert_file(service, filename, content, folder_id)
        print(f"[{protocol}] uploaded to Drive → {protocol}/{filename}")
    except Exception as e:
        print(f"[{protocol}] Drive upload failed: {e}")


# ---------------------------------------------------------------------------
# Per-protocol runner
# ---------------------------------------------------------------------------

def run_protocol(protocol, date_str, yesterday_str):
    raw_dir      = os.path.join("data", "raw", protocol)
    snapshot_dir = os.path.join("data", "brwa-snapshot", protocol)
    summary_dir  = os.path.join("data", "summaries", protocol)
    for d in (raw_dir, snapshot_dir, summary_dir):
        os.makedirs(d, exist_ok=True)

    raw_path      = os.path.join(raw_dir,      f"{date_str}.json")
    snapshot_path = os.path.join(snapshot_dir, f"{date_str}.snapshot.json")
    summary_path  = os.path.join(summary_dir,  f"{date_str}.summary.json")
    yesterday_path = os.path.join(snapshot_dir, f"{yesterday_str}.snapshot.json")

    print(f"[{protocol}] fetching tokens...")
    raw_data = fetch_protocol_tokens(protocol)
    token_list = raw_data.get("results", []) if isinstance(raw_data, dict) else raw_data
    print(f"[{protocol}] {len(token_list)} tokens received")

    with open(raw_path, "w") as f:
        json.dump(raw_data, f)

    today_snapshot = normalize_snapshot(raw_data)
    with open(snapshot_path, "w") as f:
        json.dump(today_snapshot, f, indent=2)

    yesterday_snapshot = {}
    if os.path.exists(yesterday_path):
        with open(yesterday_path) as f:
            yesterday_snapshot = json.load(f)
    else:
        print(f"[{protocol}] no yesterday snapshot — all tokens classified as new")

    summary = produce_summary(date_str, protocol, today_snapshot, yesterday_snapshot)
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(
        f"[{protocol}] summary → "
        f"{len(summary['inflates'])} inflates, "
        f"{len(summary['deflates'])} deflates, "
        f"{len(summary['new'])} new"
    )
    upload_summary_to_drive(protocol, date_str, summary_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not API_TOKEN:
        print("RWA_API_TOKEN environment variable is not set")
        sys.exit(1)

    today     = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    date_str      = today.isoformat()
    yesterday_str = yesterday.isoformat()

    print(f"Protocol summary pipeline — {date_str}")

    failed = []
    for protocol in PROTOCOLS:
        try:
            run_protocol(protocol, date_str, yesterday_str)
        except Exception as e:
            print(f"[{protocol}] FAILED: {e}")
            failed.append(protocol)

    if failed:
        print(f"\nFailed protocols: {failed}")
        sys.exit(1)
    print("\nDone!")


if __name__ == "__main__":
    main()
