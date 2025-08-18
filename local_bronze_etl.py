# bronze_local_ingest_blob_to_blob.py
import argparse, json, os, sys, time, mimetypes
from datetime import datetime, timezone, timedelta
from typing import Optional
import requests
from azure.storage.blob import (
    BlobServiceClient, BlobClient, ContentSettings,
    generate_blob_sas, BlobSasPermissions
)

def log(m): print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {m}", flush=True)

def clients():
    conn = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    src_container = os.environ["SRC_CONTAINER"]
    dst_container = os.environ["DST_CONTAINER"]
    svc = BlobServiceClient.from_connection_string(conn)
    return svc.get_container_client(src_container), svc.get_container_client(dst_container)

def content_type_for(name: str) -> str:
    c, _ = mimetypes.guess_type(name)
    return c or "application/octet-stream"

def server_side_copy(src_cc, dst_cc, src_blob: str, dst_blob: str, sas_ttl_minutes=30) -> bool:
    """Try server-side copy via SAS URL. Returns True if kicked off."""
    try:
        # Build SAS for source blob (read)
        parts = src_cc.url.split("/")
        account_url = "/".join(parts[:3])  # https://<acct>.blob.core.windows.net
        src_container = parts[-1]          # not reliable; safer to use src_cc.container_name
        src_container = src_cc.container_name

        sas = generate_blob_sas(
            account_name=src_cc.account_name,
            container_name=src_container,
            blob_name=src_blob,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(minutes=sas_ttl_minutes),
        )
        src_blob_client = src_cc.get_blob_client(src_blob)
        sas_url = f"{src_blob_client.url}?{sas}"

        dst_blob_client: BlobClient = dst_cc.get_blob_client(dst_blob)
        dst_blob_client.start_copy_from_url(sas_url)
        return True
    except Exception as e:
        log(f"Server-side copy not available ({src_blob}): {e}")
        return False

def stream_copy(src_cc, dst_cc, src_blob: str, dst_blob: str):
    data = src_cc.download_blob(src_blob).readall()
    dst_cc.upload_blob(
        name=dst_blob, data=data, overwrite=True,
        content_settings=ContentSettings(content_type=content_type_for(src_blob))
    )

def copy_prefix_to_bronze(dt: str) -> dict:
    src_cc, dst_cc = clients()
    base_prefix = os.environ.get("SRC_PREFIX", "").lstrip("/")
    dst_base = os.environ.get("DST_BASE_PREFIX", "bronze").strip("/")

    if base_prefix and not base_prefix.endswith("/"):
        base_prefix += "/"

    count = 0
    for blob in src_cc.list_blobs(name_starts_with=base_prefix):
        name = blob.name
        if name.endswith("/") or name == base_prefix:
            continue
        rel = name[len(base_prefix):] if name.startswith(base_prefix) else name
        dst_blob = f"{dst_base}/dt={dt}/source=blob/{rel}"

        # Ensure folder-like paths are preserved
        if not server_side_copy(src_cc, dst_cc, name, dst_blob):
            stream_copy(src_cc, dst_cc, name, dst_blob)
        log(f"Copied: {name}  ->  {dst_blob}")
        count += 1

    # small manifest
    manifest = {
        "source_container": src_cc.container_name,
        "source_prefix": base_prefix,
        "destination_container": dst_cc.container_name,
        "destination_prefix": f"{dst_base}/dt={dt}/source=blob/",
        "file_count": count,
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    dst_cc.upload_blob(
        name=f"{dst_base}/dt={dt}/source=blob/_manifest.json",
        data=json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    return manifest

def fetch_payments(dt: str) -> Optional[dict]:
    url_tpl = os.environ.get("PAYMENTS_API_URL")
    api_key = os.environ.get("PAYMENTS_API_KEY")
    if not url_tpl or not api_key:
        log("Payments env not set; skipping payments ingestion.")
        return None

    url = url_tpl.format(date=dt, ds=dt)
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    for attempt in range(5):
        try:
            r = requests.get(url, headers=headers, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 4: raise
            log(f"Payments API error (attempt {attempt+1}/5): {e}; retrying...")
            time.sleep(2 ** attempt)

def to_jsonl_bytes(obj) -> bytes:
    lines = []
    if isinstance(obj, list):
        lines = [json.dumps(row, ensure_ascii=False) for row in obj]
    elif isinstance(obj, dict):
        for k in ("data","results","items","payments"):
            if k in obj and isinstance(obj[k], list):
                lines = [json.dumps(row, ensure_ascii=False) for row in obj[k]]
                break
        if not lines:
            lines = [json.dumps(obj, ensure_ascii=False)]
    else:
        lines = [json.dumps({"raw": obj}, ensure_ascii=False)]
    return ("\n".join(lines) + "\n").encode("utf-8")

def ingest_payments(dt: str):
    _, dst_cc = clients()
    dst_base = os.environ.get("DST_BASE_PREFIX", "bronze").strip("/")
    base = f"{dst_base}/dt={dt}/source=payments"

    payload = fetch_payments(dt)
    if payload is None:
        return {"skipped": True}

    jsonl = to_jsonl_bytes(payload)
    dst_cc.upload_blob(
        name=f"{base}/payments_{dt}.jsonl", data=jsonl, overwrite=True,
        content_settings=ContentSettings(content_type="application/x-ndjson"),
    )
    manifest = {
        "endpoint": os.environ.get("PAYMENTS_API_URL"),
        "date": dt,
        "row_count_estimate": len(jsonl.splitlines()),
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    dst_cc.upload_blob(
        name=f"{base}/_manifest_{dt}.json",
        data=json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    return {"rows": manifest["row_count_estimate"]}

def main():
    parser = argparse.ArgumentParser(description="Copy Blob→Blob to Bronze; optional Payments API ingest")
    parser.add_argument("--date", help="Partition date YYYY-MM-DD (default: today UTC)")
    args = parser.parse_args()
    dt = args.date or datetime.utcnow().strftime("%Y-%m-%d")
    log(f"Start Bronze (Blob→Blob) dt={dt}")

    try:
        mani = copy_prefix_to_bronze(dt)
        log(f"Blob copy summary: {mani}")
    except Exception as e:
        log(f"Blob copy FAILED: {e}")
        sys.exit(2)

    try:
        res = ingest_payments(dt)
        log(f"Payments summary: {res}")
    except Exception as e:
        log(f"Payments ingestion FAILED: {e}")
        # non-fatal if blob copy succeeded

    log("Done.")

if __name__ == "__main__":
    main()
