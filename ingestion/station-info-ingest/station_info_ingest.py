import os
import json
import urllib.request
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ.get("DATASET", "raw")
TABLE = os.environ.get("TABLE", "station_info")
FEED_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

# region_id kept ONLY as a harmless column. City is derived later in dbt from
# lat/lon (region_id is opaque + multi-valued + nullable => banned as city key).
SCHEMA = [
    bigquery.SchemaField("station_id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("lat", "FLOAT"),
    bigquery.SchemaField("lon", "FLOAT"),
    bigquery.SchemaField("capacity", "INTEGER"),
    bigquery.SchemaField("region_id", "STRING"),
    bigquery.SchemaField("ingest_ts", "TIMESTAMP"),
]


def fetch_stations():
    with urllib.request.urlopen(FEED_URL, timeout=30) as r:
        payload = json.load(r)
    return payload["data"]["stations"]


def main():
    stations = fetch_stations()
    now = datetime.now(timezone.utc).isoformat()
    rows = [{
        "station_id": s.get("station_id"),
        "name": s.get("name"),
        "lat": s.get("lat"),
        "lon": s.get("lon"),
        "capacity": s.get("capacity"),
        "region_id": s.get("region_id"),
        "ingest_ts": now,
    } for s in stations]

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(rows)} stations -> {table_id}")


if __name__ == "__main__":
    main()