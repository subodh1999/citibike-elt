import os, json, datetime, urllib.request, urllib.parse
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ.get("DATASET", "raw")
TABLE = os.environ.get("TABLE", "weather")
START_DATE = os.environ.get("START_DATE", "2024-06-01")
END_DATE = os.environ.get("END_DATE", "2026-05-31")

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY = ["temperature_2m", "relative_humidity_2m", "precipitation",
          "wind_speed_10m", "snowfall"]
CITIES = [
    {"city": "nyc", "lat": 40.7128, "lon": -74.0060},
    {"city": "jc",  "lat": 40.7178, "lon": -74.0431},
]

def fetch_city(c):
    params = {
        "latitude": c["lat"], "longitude": c["lon"],
        "start_date": START_DATE, "end_date": END_DATE,
        "hourly": ",".join(HOURLY), "timezone": "UTC",
    }
    url = ARCHIVE_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        payload = json.load(r)
    h = payload.get("hourly", {})
    times = h.get("time", [])
    ingest_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows = []
    for i, t in enumerate(times):
        rows.append({
            "city": c["city"], "latitude": c["lat"], "longitude": c["lon"],
            "time": t,
            "temperature_2m": h.get("temperature_2m", [])[i] if h.get("temperature_2m") else None,
            "relative_humidity_2m": h.get("relative_humidity_2m", [])[i] if h.get("relative_humidity_2m") else None,
            "precipitation": h.get("precipitation", [])[i] if h.get("precipitation") else None,
            "wind_speed_10m": h.get("wind_speed_10m", [])[i] if h.get("wind_speed_10m") else None,
            "snowfall": h.get("snowfall", [])[i] if h.get("snowfall") else None,
            "ingest_ts": ingest_ts,
        })
    return rows

def main():
    rows = []
    for c in CITIES:
        cr = fetch_city(c)
        print(f"{c['city']}: {len(cr)} hourly rows", flush=True)
        rows.extend(cr)

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    schema = [
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("time", "STRING"),
        bigquery.SchemaField("temperature_2m", "FLOAT"),
        bigquery.SchemaField("relative_humidity_2m", "FLOAT"),
        bigquery.SchemaField("precipitation", "FLOAT"),
        bigquery.SchemaField("wind_speed_10m", "FLOAT"),
        bigquery.SchemaField("snowfall", "FLOAT"),
        bigquery.SchemaField("ingest_ts", "TIMESTAMP"),
    ]
    cfg = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    client.load_table_from_json(rows, table_id, job_config=cfg).result()
    print(f"loaded {len(rows)} rows into {table_id}", flush=True)

if __name__ == "__main__":
    main()
