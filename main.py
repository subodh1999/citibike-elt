import os, json, datetime, urllib.request
from concurrent import futures
from flask import Flask
from google.cloud import pubsub_v1

PROJECT_ID = os.environ["PROJECT_ID"]
TOPIC_ID = os.environ.get("TOPIC_ID", "gbfs-station-status")
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@app.route("/", methods=["GET", "POST"])
def fetch_and_publish():
    ingest_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    req = urllib.request.Request(STATUS_URL, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.load(resp)

    feed_last_updated = payload.get("last_updated")
    stations = payload.get("data", {}).get("stations", [])

    pub_futures = []
    for st in stations:
        st["ingest_ts"] = ingest_ts
        st["feed_last_updated"] = feed_last_updated
        pub_futures.append(publisher.publish(topic_path, json.dumps(st).encode("utf-8")))

    futures.wait(pub_futures, return_when=futures.ALL_COMPLETED)
    return {"published": len(pub_futures), "ingest_ts": ingest_ts}, 200