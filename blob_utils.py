# blob_utils.py
import os
import re
from datetime import datetime, date, timedelta, timezone
from typing import Iterable, Dict, List, Optional, Tuple
from azure.storage.blob import ContainerClient

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
