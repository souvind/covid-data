# extract.py
# Simple script to fetch COVID-19 summary from a public API and save locally.
import requests, json, os
from pathlib import Path

SAMPLE_DIR = Path("sample_data")
SAMPLE_DIR.mkdir(exist_ok=True)

API_URL = "https://api.covid19api.com/summary"

def fetch_and_save(url=API_URL, out_path=SAMPLE_DIR / "covid_summary.json"):
    print(f"Fetching data from {url} ...")
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"Saved sample to {out_path}")
    except Exception as e:
        print(f"Failed to fetch data: {e}")
        # create a small fallback sample
        fallback = {
            "Global": {"NewConfirmed": 0, "TotalConfirmed": 0},
            "Countries": [
                {"Country": "Sampleland", "CountryCode": "SL", "NewConfirmed": 1, "TotalConfirmed": 10,
                 "NewDeaths": 0, "TotalDeaths": 0, "NewRecovered": 0, "TotalRecovered": 5, "Date": "2023-01-01T00:00:00Z"}
            ]
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2)
        print(f"Wrote fallback sample to {out_path}")

if __name__ == '__main__':
    fetch_and_save()
