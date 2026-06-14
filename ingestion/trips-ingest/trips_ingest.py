import os, re, io, zipfile, urllib.request, xml.etree.ElementTree as ET
from google.cloud import storage

S3_URL = "https://s3.amazonaws.com/tripdata/"
GCS_BUCKET = os.environ["BUCKET"]
START = int(os.environ.get("START_YYYYMM", "202406"))
END = int(os.environ.get("END_YYYYMM", "209912"))
TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))

NAME_RE = re.compile(r"^(JC-)?(\d{6})-citibike-tripdata\.(?:csv\.)?zip$")
gcs = storage.Client()
bucket = gcs.bucket(GCS_BUCKET)

def list_zips():
    req = urllib.request.Request(S3_URL, headers={"Accept": "application/xml"})
    with urllib.request.urlopen(req, timeout=60) as r:
        root = ET.fromstring(r.read())
    keys = [e.text for e in root.iter() if e.tag.endswith("Key")]
    picked = []
    for k in keys or []:
        m = NAME_RE.match(k or "")
        if m and START <= int(m.group(2)) <= END:
            picked.append(k)
    return sorted(picked)

def process(key):
    city = "jc" if key.startswith("JC-") else "nyc"
    print(f"[task {TASK_INDEX}] downloading {key}", flush=True)
    with urllib.request.urlopen(S3_URL + key, timeout=600) as r:
        buf = io.BytesIO(r.read())
    with zipfile.ZipFile(buf) as zf:
        for member in zf.namelist():
            if not member.lower().endswith(".csv") or member.startswith("__MACOSX"):
                continue
            dest = f"trips/{city}/{os.path.basename(member)}"
            print(f"[task {TASK_INDEX}] -> gs://{GCS_BUCKET}/{dest}", flush=True)
            with zf.open(member) as src:
                bucket.blob(dest).upload_from_file(src, content_type="text/csv")

def main():
    keys = list_zips()
    mine = [k for i, k in enumerate(keys) if i % TASK_COUNT == TASK_INDEX]
    print(f"[task {TASK_INDEX}/{TASK_COUNT}] handling {len(mine)} of {len(keys)} files", flush=True)
    for k in mine:
        process(k)
    print(f"[task {TASK_INDEX}] done", flush=True)

if __name__ == "__main__":
    main()