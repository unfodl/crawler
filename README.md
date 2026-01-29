# RWA Holders Data Crawler

Automated daily snapshots of RWA token holder counts from the Nest protocol.

## Data Source

- **API**: https://rwa-api-production.up.railway.app/rwalegacy
- **Protocol**: Nest
- **Measure**: Holding addresses count

## Files

- `snapshots/rwa_holders_daily.csv` - Main database with all historical holder counts
- `snapshots/raw_YYYY-MM-DD.json` - Raw API responses for debugging
- `fetch_rwa_data.py` - Python script to fetch and merge data

## CSV Structure

The CSV file has one row per date with columns:
- `date` - Date in YYYY-MM-DD format
- One column per token (e.g., `Plume nELIXIR`, `Plume nCREDIT`, etc.)

This format makes it easy to create stacked area charts showing holder growth over time.

## GitHub Actions

The workflow runs every 12 hours (00:00 and 12:00 UTC) and:
1. Fetches latest data from the API
2. Merges with existing CSV data
3. Uploads to Dune Analytics
4. Commits changes if any new data

### Setup

Add the Dune API key as a repository secret:
1. Go to your repo → Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `DUNE_API_KEY`
4. Value: Your Dune API key

### Dune Table

Data is uploaded to: `dune.plume.dataset_rwa_holders_daily`

### Manual Trigger

You can manually trigger the workflow from the Actions tab in GitHub.

## Local Development

```bash
# Run the script locally
python fetch_rwa_data.py
```

## Example: Creating a Stacked Chart

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('snapshots/rwa_holders_daily.csv', index_col='date', parse_dates=True)
df.fillna(0).plot.area(stacked=True, figsize=(12, 6))
plt.title('RWA Token Holders Over Time')
plt.ylabel('Number of Holders')
plt.show()
```
