"""Download HM Land Registry's latest monthly Price Paid Data update (~16 MB).

HM Land Registry republishes this file every month, so the script also writes
`data/raw/source.json` with the server's Last-Modified date, size and SHA-256.
That pins which release the results came from, and `run_local.py` uses the
Last-Modified month as the release label.

Licence: Contains HM Land Registry data © Crown copyright and database right
2026. This data is licensed under the Open Government Licence v3.0.
"""
import hashlib
import json
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/"
    "pp-monthly-update-new-version.csv"
)
RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
OUT = RAW / "pp-monthly-update.csv"
META = RAW / "source.json"


def main(force: bool = False) -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    if OUT.exists() and META.exists() and not force:
        print(f"already present: {OUT} (use --force to re-download)")
        return
    with urllib.request.urlopen(URL, timeout=120) as resp:
        payload = resp.read()
        last_modified = parsedate_to_datetime(resp.headers["Last-Modified"])
    OUT.write_bytes(payload)
    meta = {
        "url": URL,
        "last_modified": last_modified.isoformat(),
        "release": last_modified.strftime("%Y-%m"),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    META.write_text(json.dumps(meta, indent=2) + "\n")
    print(f"saved {OUT} ({len(payload):,} bytes), release {meta['release']}")


if __name__ == "__main__":
    import sys

    main(force="--force" in sys.argv)
