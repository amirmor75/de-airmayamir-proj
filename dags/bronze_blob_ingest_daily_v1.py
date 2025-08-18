from __future__ import annotations
from datetime import datetime, date, timedelta, timezone
import json
import os
import re
from typing import Iterable, Dict, List, Optional, Tuple

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from azure.storage.blob import BlobServiceClient, ContainerClient



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

# -------- Logging --------
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

# -------- Date parsing/normalization --------
# Note: %d/%m/%Y is optional—uncomment if needed.
_DATE_PATTERNS = [
    "%Y-%m-%d",   # 2025-08-18
    "%d.%m.%Y",   # 18.08.2025 or 18.8.2025 (Python tolerates single-digit)
    "%d-%m-%Y",   # 18-08-2025 / 18-8-2025
    # "%d/%m/%Y", # 18/08/2025
]

def parse_any_date_token(s: str) -> Optional[date]:
    s = s.strip()
    if not re.fullmatch(r"[0-9.\-\/]+", s):
        return None
    for fmt in _DATE_PATTERNS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def to_iso_day(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_iso_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

# -------- Blob listing helpers --------
def list_blob_names(container: ContainerClient, prefix: str) -> Iterable[str]:
    return (b.name for b in container.list_blobs(name_starts_with=prefix))

def list_data_types(container: ContainerClient, root_prefix: str) -> List[str]:
    """Return unique first-level segments under AirTech_FinalProject12/ -> data types."""
    seen = set()
    base = root_prefix.rstrip("/") + "/"
    for name in list_blob_names(container, base):
        remainder = name[len(base):]
        seg = remainder.split("/", 1)[0]
        if seg:
            seen.add(seg)
    return sorted(seen)

def find_nearby_dates(
    container: ContainerClient,
    root_prefix: str,
    data_type: str,
    target_iso_day: str,
    window: int = 5,
) -> List[str]:
    """Return up to `window` existing dates (as ISO) closest to target."""
    base = f"{root_prefix.rstrip('/')}/{data_type}/"
    rx = re.compile(r"^" + re.escape(base) + r"([^/]+)/")
    found_tokens = set()

    for name in list_blob_names(container, base):
        m = rx.match(name)
        if m:
            found_tokens.add(m.group(1))

    # Parse to dates, keep those we recognize
    dates: List[date] = []
    for tok in found_tokens:
        d = parse_any_date_token(tok)
        if d:
            dates.append(d)

    target = parse_iso_day(target_iso_day)
    ranked = sorted(dates, key=lambda d: abs((d - target).days))
    return [to_iso_day(d) for d in ranked[:window]]

def resolve_source_date_folder(
    container: ContainerClient,
    root_prefix: str,
    data_type: str,
    wanted_iso_day: str,
) -> Optional[str]:
    """
    Find the actual source folder name for the requested date under:
      root/<data_type>/<date_folder>/
    The folder may be '2025-08-18' or '18.8.2025' etc.
    Returns the *folder token* to use in the source path, or None if not found.
    """
    base = f"{root_prefix.rstrip('/')}/{data_type}/"
    target = parse_iso_day(wanted_iso_day)

    # Collect first-level date-folder tokens
    tokens = set()
    for name in list_blob_names(container, base):
        rest = name[len(base):]
        seg = rest.split("/", 1)[0]
        if seg:
            tokens.add(seg)

    # Exact token match first
    if wanted_iso_day in tokens:
        return wanted_iso_day

    # Parse all tokens; return the one equal to target
    for tok in tokens:
        d = parse_any_date_token(tok)
        if d == target:
            return tok

    return None

# -------- Copying helpers --------
def copy_blobs_for_date(
    src_container: ContainerClient,
    dst_container: ContainerClient,
    root_prefix: str,
    data_type: str,
    wanted_iso_day: str,
    dst_root_prefix: str ,
) -> Tuple[int, Optional[str]]:
    """
    Copy all blobs from:
      src:  {root_prefix}/{data_type}/{resolved_date_token}/...
    into:
      dst:  {dst_root_prefix}/dt={wanted_iso_day}/source=blob/{data_type}/<basename>

    Returns (count, resolved_token_used). resolved_token_used may differ from wanted_iso_day
    if the source folder uses another date format.
    """
    resolved = resolve_source_date_folder(src_container, root_prefix, data_type, wanted_iso_day)
    if not resolved:
        return (0, None)

    src_prefix = f"{root_prefix.rstrip('/')}/{data_type}/{resolved}/"
    copied = 0
    for blob in src_container.list_blobs(name_starts_with=src_prefix):
        src_blob = src_container.get_blob_client(blob.name)

        y, m, d = wanted_iso_day.split("-")  # YYYY, MM, DD
        basename = os.path.basename(blob.name)
        dst_dir = (
            f"{dst_root_prefix.rstrip('/')}/"
            f"{data_type}/year={y}/month={m}/day={d}/source=blob/"
        )
        dst_name = f"{dst_dir}{basename}"
        dst_blob = dst_container.get_blob_client(dst_name)
        # Server-side copy; avoids local download
        dst_blob.start_copy_from_url(src_blob.url)
        copied += 1

    return (copied, resolved)

def copy_all_types_for_day(
    src_container: ContainerClient,
    dst_container: ContainerClient,
    root_prefix: str,
    wanted_iso_day: str,
    dst_root_prefix: str,
    fallback_to_yesterday: bool = True,
) -> Dict:
    """
    High-level orchestration for a single day across all discovered data types.
    """
    
    data_types = list_data_types(src_container, root_prefix)
    if not data_types:
        log(f"ERROR: No data-types under '{root_prefix}/' in source container.")
        return {
            "date_token": wanted_iso_day,
            "file_count": 0,
            "per_type_counts": {},
            "data_types_included": [],
            "resolved_tokens": {},
        }

    per_type_counts: Dict[str, int] = {}
    resolved_tokens: Dict[str, Optional[str]] = {}
    total = 0

    # First pass: requested day
    for dtp in data_types:
        n, resolved = copy_blobs_for_date(
            src_container, dst_container, root_prefix, dtp, wanted_iso_day, dst_root_prefix
        )
        per_type_counts[dtp] = n
        resolved_tokens[dtp] = resolved
        total += n
        msg = "FOUND" if resolved else "missing"
        log(f"[{dtp}] {root_prefix}/{dtp}/{wanted_iso_day}/ -> {msg} "
            f"(resolved={resolved or '—'}, copied={n})")

    # Optional fallback to yesterday if nothing copied at all
    if total == 0 and fallback_to_yesterday:
        yest_iso = to_iso_day(parse_iso_day(wanted_iso_day) - timedelta(days=1))
        log(f"No blobs for {wanted_iso_day}. Trying fallback date {yest_iso}.")
        for dtp in data_types:
            n, resolved = copy_blobs_for_date(
                src_container, dst_container, root_prefix, dtp, yest_iso, dst_root_prefix
            )
            if n:
                per_type_counts[dtp] += n
                resolved_tokens[dtp] = resolved
                total += n
            else:
                near = find_nearby_dates(src_container, root_prefix, dtp, wanted_iso_day, window=5)
                if near:
                    log(f"[{dtp}] Nearby existing dates: {', '.join(near)}")

    summary = {
        "date_token": wanted_iso_day,
        "destination_prefix_root": f"{dst_root_prefix.rstrip('/')}/dt={wanted_iso_day}/source=blob/",
        "file_count": total,
        "per_type_counts": per_type_counts,
        "data_types_included": [k for k, v in per_type_counts.items() if v > 0],
        "resolved_tokens": resolved_tokens,
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return summary


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
