# bronze_local_ingest_blob_to_blob.py
import os

import argparse
from datetime import  datetime, timezone

from azure.storage.blob import BlobServiceClient
from blob_utils import log, copy_all_types_for_day
from dotenv import load_dotenv

load_dotenv()


def env_bool(name: str, default: str = "true") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "y")

def main():
    parser = argparse.ArgumentParser(description="Copy bronze partitions from Blob→Blob with date normalization.")
    parser.add_argument("--dt", required=False, help="Target date in ISO (YYYY-MM-DD). Defaults to today.")
    args = parser.parse_args()

    date_token = args.dt or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    conn = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    src_container_name = os.environ["SRC_CONTAINER"]
    dst_container_name = os.environ["DST_CONTAINER"]
    root_prefix = os.environ.get("SRC_PREFIX")
    dst_root_prefix = os.environ.get("DST_PREFIX_ROOT")
    fallback = env_bool("FALLBACK_TO_YESTERDAY")

    log(f"Start Bronze (Blob→Blob) dt={date_token}")
    svc = BlobServiceClient.from_connection_string(conn)
    src_cc = svc.get_container_client(src_container_name)
    dst_cc = svc.get_container_client(dst_container_name)

    summary = copy_all_types_for_day(
        src_container=src_cc,
        dst_container=dst_cc,
        root_prefix=root_prefix,
        wanted_iso_day=date_token,
        dst_root_prefix=dst_root_prefix,
        fallback_to_yesterday=fallback,
    )
    
    summary.update({
        "source_container": src_container_name,
        "destination_container": dst_container_name,
    })
    log(f"Blob copy summary: {summary}")

    if os.environ.get("PAYMENTS_API_URL"):
        log("Payments ingestion configured; implement as needed.")
    else:
        log("Payments env not set; skipping payments ingestion.")

    log("Done.")

if __name__ == "__main__":
    main()
