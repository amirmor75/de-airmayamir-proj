import io
import json
import os
from datetime import datetime
from typing import Iterable, Dict, Any, List

import requests
from dateutil.parser import isoparse
from azure.storage.blob import BlobServiceClient, ContentSettings

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



# --------------------------------------
# Environment variables (set in Airflow or a Secret Backend)

# AZURE_STORAGE_CONNECTION_STRING

# AZURE_CONTAINER (e.g., raw-bronze)

# GDRIVE_SA_JSON (inline JSON string or path to file; see code)

# GDRIVE_FILE_IDS (comma-separated file IDs) or GDRIVE_FOLDER_IDS (comma-separated)

# PAYMENTS_API_URL (e.g., https://api.vendor.com/v1/payments?date={ds})

# PAYMENTS_API_KEY (Bearer or vendor key)
# --------------------------------------

# ---------- Utilities: Azure ----------
def _blob_client():
    conn = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    container = os.environ["AZURE_CONTAINER"]
    svc = BlobServiceClient.from_connection_string(conn)
    return svc.get_container_client(container)

def _upload_bytes(container_client, blob_path: str, data: bytes, content_type: str = "application/octet-stream"):
    container_client.upload_blob(
        name=blob_path,
        data=data,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )
    return blob_path

# ---------- Utilities: Google Drive ----------
def _gdrive_service():
    # Supports either inline JSON via env, or file path
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    sa_json = os.environ.get("GDRIVE_SA_JSON")
    if not sa_json:
        raise RuntimeError("GDRIVE_SA_JSON env var required (inline JSON or @/path/to/file.json).")
    if sa_json.startswith("@"):  # treat as path
        with open(sa_json[1:], "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        info = json.loads(sa_json)

    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _list_files_in_folders(drive, folder_ids: Iterable[str]) -> List[Dict[str, Any]]:
    files = []
    for folder_id in folder_ids:
        page_token = None
        q = f"'{folder_id}' in parents and trashed = false"
        while True:
            resp = drive.files().list(
                q=q,
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                pageToken=page_token,
                pageSize=1000,
            ).execute()
            files.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    return files

def _download_drive_file_as_bytes(drive, file_obj: Dict[str, Any]) -> (bytes, str):
    """
    Returns (content_bytes, content_type). Exports Google Sheets as CSV.
    """
    from googleapiclient.http import MediaIoBaseDownload

    file_id = file_obj["id"]
    mime = file_obj["mimeType"]
    # Google Sheets export
    if mime == "application/vnd.google-apps.spreadsheet":
        request = drive.files().export_media(fileId=file_id, mimeType="text/csv")
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue(), "text/csv"

    # Generic binary download
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    # Best-effort content type mapping
    content_type = "application/octet-stream"
    if file_obj["name"].lower().endswith(".csv"):
        content_type = "text/csv"
    elif file_obj["name"].lower().endswith(".json"):
        content_type = "application/json"
    elif file_obj["name"].lower().endswith(".parquet"):
        content_type = "application/octet-stream"
    return buf.getvalue(), content_type

# ---------- Utilities: Payments API ----------
def _fetch_payments_payload(ds: str) -> Dict[str, Any]:
    url_template = os.environ["PAYMENTS_API_URL"]
    url = url_template.format(ds=ds)
    headers = {
        "Accept": "application/json",
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def _normalize_payments_to_jsonl(obj: Any) -> bytes:
    """
    Accepts list/dict, returns JSONL bytes. Pass-through if already list of rows.
    """
    lines = []
    if isinstance(obj, list):
        for row in obj:
            lines.append(json.dumps(row, ensure_ascii=False))
    elif isinstance(obj, dict):
        # common patterns
        for key in ("data", "results", "items"):
            if key in obj and isinstance(obj[key], list):
                for row in obj[key]:
                    lines.append(json.dumps(row, ensure_ascii=False))
                break
        else:
            # single object fallback
            lines.append(json.dumps(obj, ensure_ascii=False))
    else:
        lines.append(json.dumps({"raw": obj}, ensure_ascii=False))
    return ("\n".join(lines) + "\n").encode("utf-8")

# ---------- Airflow DAG ----------
DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
}

@dag(
    dag_id="bronze_ingest_drive_and_payments",
    schedule="0 3 * * *",  # daily 03:00
    start_date=datetime(2025, 8, 1),
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["bronze", "ingest", "azure", "drive", "payments"],
)
def bronze_ingest():
    @task
    def ingest_drive_files(logical_date: str):
        ds = logical_date[:10]  # 'YYYY-MM-DD'
        container = _blob_client()
        drive = _gdrive_service()

        # Gather file list either from explicit file IDs or folder IDs
        file_ids_env = os.environ.get("GDRIVE_FILE_IDS", "").strip()
        folder_ids_env = os.environ.get("GDRIVE_FOLDER_IDS", "").strip()

        files: List[Dict[str, Any]] = []

        if folder_ids_env:
            folder_ids = [f.strip() for f in folder_ids_env.split(",") if f.strip()]
            files.extend(_list_files_in_folders(drive, folder_ids))

        if file_ids_env:
            ids = [f.strip() for f in file_ids_env.split(",") if f.strip()]
            for fid in ids:
                meta = drive.files().get(fileId=fid, fields="id, name, mimeType, modifiedTime").execute()
                files.append(meta)

        if not files:
            return {"ingested": 0, "blobs": []}

        results = []
        for fobj in files:
            data, content_type = _download_drive_file_as_bytes(drive, fobj)
            name = fobj["name"]
            # Stable, audit-friendly path
            modified_iso = fobj.get("modifiedTime")
            modified = isoparse(modified_iso).strftime("%Y-%m-%dT%H:%M:%SZ") if modified_iso else "unknown"
            blob_path = f"bronze/dt={ds}/source=drive/fileId={fobj['id']}/{name}"
            _upload_bytes(container, blob_path, data, content_type=content_type)

            # Also write a small sidecar metadata JSON
            meta_path = f"bronze/dt={ds}/source=drive/fileId={fobj['id']}/_meta.json"
            meta_doc = json.dumps(
                {"id": fobj["id"], "name": name, "mimeType": fobj["mimeType"], "modifiedTime": modified},
                ensure_ascii=False,
            ).encode("utf-8")
            _upload_bytes(container, meta_path, meta_doc, content_type="application/json")

            results.append(blob_path)
        return {"ingested": len(results), "blobs": results}

    @task
    def ingest_payments_api(logical_date: str):
        ds = logical_date[:10]
        container = _blob_client()

        payload = _fetch_payments_payload(ds)
        jsonl = _normalize_payments_to_jsonl(payload)

        blob_path = f"bronze/dt={ds}/source=payments/payments_{ds}.jsonl"
        _upload_bytes(container, blob_path, jsonl, content_type="application/x-ndjson")

        # Attach a small lineage manifest
        manifest = {
            "endpoint": os.environ.get("PAYMENTS_API_URL"),
            "date": ds,
            "row_count_estimate": len(jsonl.splitlines()),
            "ingested_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        _upload_bytes(
            container,
            f"bronze/dt={ds}/source=payments/_manifest_{ds}.json",
            json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
        )
        return {"blob": blob_path, "rows": manifest["row_count_estimate"]}

    drive_task = ingest_drive_files.logical_date  # type: ignore[attr-defined]
    payments_task = ingest_payments_api.logical_date  # type: ignore[attr-defined]

    trigger_silver = TriggerDagRunOperator(
    task_id="trigger_silver_blob_dag",
    trigger_dag_id="silver_blob_dag",   # must match the DAG ID of the downstream DAG
    wait_for_completion=False,          # set True if you want bronze DAG to wait until silver finishes
)

# Wire dependencies: run only after both ingestion tasks succeed
    [drive_task(), payments_task()] >> trigger_silver
    
dag_obj = bronze_ingest()
