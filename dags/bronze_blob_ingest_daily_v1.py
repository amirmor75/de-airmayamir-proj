from __future__ import annotations
from datetime import timedelta
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from azure.storage.blob import BlobServiceClient
from blob_utils import log, copy_all_types_for_day


def _v(key: str, default: str | None = None, *, required=False) -> str:
    val = Variable.get(key, default_var=default)
    if required and (val is None or val == ""):
        raise ValueError(f"Missing required Airflow Variable: {key}")
    return val

AZ_CONN_STR        = _v("AZURE_STORAGE_CONNECTION_STRING", required=True)
SRC_CONTAINER      = _v("SRC_CONTAINER", required=True)
DST_CONTAINER      = _v("DST_CONTAINER", required=True)
SRC_ROOT_PREFIX    = _v("SRC_ROOT_PREFIX", "AirTech_FinalProject12")
DST_PREFIX_ROOT    = _v("DST_PREFIX_ROOT", "bronze")
FALLBACK_YESTERDAY = _v("FALLBACK_TO_YESTERDAY", "true").lower() in ("1","true","yes","y")

# Currency API config (optional)
CURRENCY_API_URL   = _v("CURRENCY_API_URL", "")
CURRENCY_API_KEY   = _v("CURRENCY_API_KEY", "")
CURRENCY_DT        = _v("CURRENCY_DATA_TYPE", "currency")
CURRENCY_KEY_HEADER= _v("CURRENCY_KEY_HEADER", "apikey")  # or set "" to skip header

# -------------------------
# Helpers
# -------------------------
def _y_m_d_from_iso_day(iso_day: str) -> tuple[str, str, str]:
    y, m, d = iso_day.split("-")
    return y, m, d

def _dst_dir_for(data_type: str, iso_day: str, source_tag: str) -> str:
    y, m, d = _y_m_d_from_iso_day(iso_day)
    return f"{DST_PREFIX_ROOT.rstrip('/')}/{data_type}/year={y}/month={m}/day={d}/source={source_tag}/"

def _get_clients():
    svc = BlobServiceClient.from_connection_string(AZ_CONN_STR)
    return svc.get_container_client(SRC_CONTAINER), svc.get_container_client(DST_CONTAINER)

# -------------------------
# Tasks
# -------------------------
def task_blob_copy_for_ds(ds: str, **_):
    log(f"[bronze] Start Blob→Blob for ds={ds}")
    src_cc, dst_cc = _get_clients()

    summary = copy_all_types_for_day(
        src_container=src_cc,
        dst_container=dst_cc,
        root_prefix=SRC_ROOT_PREFIX,
        wanted_iso_day=ds,
        dst_root_prefix=DST_PREFIX_ROOT,         # e.g., airmayamir1/bronze
        fallback_to_yesterday=FALLBACK_YESTERDAY,
    )
    summary["destination_prefix_note"] = f"{DST_PREFIX_ROOT}/<data-type>/year=YYYY/month=MM/day=DD/source=blob/"
    summary["source_container"] = SRC_CONTAINER
    summary["destination_container"] = DST_CONTAINER
    log(f"[bronze] Blob copy summary: {summary}")

    if summary["file_count"] == 0 and not FALLBACK_YESTERDAY:
        raise ValueError(f"No blobs copied for ds={ds}. Check source or enable FALLBACK_TO_YESTERDAY.")

def task_currency_api_for_ds(ds: str, **_):
    """
    Calls the currency API (URL already includes key/params) and uploads the JSON
    response under data-type = 'currency', in the same yyyy/mm/dd partition layout.
    """
    if not CURRENCY_API_URL:
        log("[currency] CURRENCY_API_URL not set; skipping.")
        return

    import requests
    from azure.storage.blob import ContentSettings

    log(f"[currency] Fetching API for ds={ds} from {CURRENCY_API_URL}")
    # Fire the request exactly as given (no auth headers added)
    resp = requests.get(CURRENCY_API_URL, timeout=60)
    resp.raise_for_status()

    # Save the payload exactly as returned
    try:
        payload = resp.json()
        bytes_out = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    except ValueError:
        # If the endpoint returns non-JSON (edge case), store raw bytes
        bytes_out = resp.content

    # Upload to bronze under data-type 'currency'
    _, dst_cc = _get_clients()
    dst_dir = _dst_dir_for("currency", ds, source_tag="api")
    blob_name = f"{dst_dir}currency_{ds}.json"  # deterministic name per day

    log(f"[currency] Uploading to {DST_CONTAINER}/{blob_name}")
    dst_cc.get_blob_client(blob_name).upload_blob(
        bytes_out,
        overwrite=True,  # overwrite same day if re-run
        content_settings=ContentSettings(content_type="application/json"),
    )
    log("[currency] Done.")
    
# -------------------------
# DAG
# -------------------------
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="AAir_mayamir_bronze_blob_ingest_daily",
    description="Daily: copy source blobs → bronze partitions; save currency API; trigger silver.",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "blob", "api", "daily"],
) as dag:

    copy_blob_sources = PythonOperator(
        task_id="copy_blob_sources_to_bronze",
        python_callable=task_blob_copy_for_ds,
        op_kwargs={"ds": "{{ ds }}"},
    )

    save_currency_api = PythonOperator(
        task_id="save_currency_api_to_bronze",
        python_callable=task_currency_api_for_ds,
        op_kwargs={"ds": "{{ ds }}"},
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_blob_dag",
        trigger_dag_id="airmayamir_temp_etl_dag",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    [copy_blob_sources, save_currency_api] >> trigger_silver
