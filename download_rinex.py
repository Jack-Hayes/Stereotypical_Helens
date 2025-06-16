#!/usr/bin/env python3

# WARNING: This script is a work in progress and is not yet ready for use
"""
Unified GNSS RINEX Downloader Script

This script:
1. Queries UNAVCO/EarthScope GNSS metadata for stations within a bounding box.
2. Extracts station IDs and their operational periods (session_start_time, session_stop_time).
3. Optionally filters stations by a provided station list.
4. Downloads daily RINEX observation files for each station over a specified date range,
   limited to each station's operational period to reduce 404 attempts.
5. Tries multiple filename patterns (lowercase/uppercase station codes) and handles retries.
6. Logs progress, summaries, and errors in a clear, concise manner.

Usage:
    python download_unified.py \
        --min-lat 46.11 --max-lat 46.27 \
        --min-lon -122.30 --max-lon -122.05 \
        --start-date 2005-01-01 --end-date 2025-05-05 \
        --out-root /path/to/rinex_data \
        --workers 5 \
        [--stations-file stations.txt] \
        [--dry-run]

Arguments:
    --min-lat, --max-lat, --min-lon, --max-lon: bounding box coordinates for station metadata query.
    --stations-file: optional file with one station ID per line to restrict download to those stations.
    --start-date, --end-date: date range for RINEX download (YYYY-MM-DD).
    --out-root: root directory to store downloaded RINEX files.
    --workers: number of concurrent download workers.
    --dry-run: if set, only logs URLs that would be tried, does not download.

Ensure environment variable EARTHSCOPE_TOKEN is set:
    export EARTHSCOPE_TOKEN=$(es sso access --token)

Note: This script uses UNAVCO metadata API via bounding-box query, avoiding unsupported 'site=' queries.
"""

import os
import sys
import requests
import logging
import threading
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

# ========== Configuration ==========
TOKEN_ENV_VAR = "EARTHSCOPE_TOKEN"
MAX_WORKERS_DEFAULT = 5
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 5
HTTP_TIMEOUT = 60
USER_AGENT = "GNSS-RINEX-Downloader/Unified/1.1"

# ========== Logging Setup ==========
logger = logging.getLogger("RINEXDownloaderUnified")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

# ========== Helper Functions ==========

def get_bearer_token():
    token = os.environ.get(TOKEN_ENV_VAR)
    if not token:
        raise RuntimeError(f"Environment variable {TOKEN_ENV_VAR} not set. "
                           "Obtain token via 'es sso access --token' and export it.")
    return token

def query_metadata_bbox(min_lat, max_lat, min_lon, max_lon):
    """
    Query UNAVCO GNSS metadata API for stations within bounding box.
    Returns a pandas DataFrame with JSON fields, including 'id', 'session_start_time', 'session_stop_time', 'latitude', 'longitude'.
    """
    base_url = "https://web-services.unavco.org/gps/metadata/sites/v1"
    params = {
        "minlatitude": min_lat,
        "maxlatitude": max_lat,
        "minlongitude": min_lon,
        "maxlongitude": max_lon
    }
    headers = {"Accept": "application/json"}
    try:
        resp = requests.get(base_url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch bounding-box metadata: {e}")
        sys.exit(1)
    data = resp.json()
    if not isinstance(data, list):
        logger.error("Unexpected metadata response format; expected a list of station records.")
        sys.exit(1)
    df = pd.DataFrame(data)
    if 'id' not in df.columns:
        logger.error("Metadata response does not contain 'id' field.")
        sys.exit(1)
    return df

def load_station_list(file_path):
    """
    Load station IDs from a text file, one per line.
    """
    if not os.path.isfile(file_path):
        logger.error(f"Stations file not found: {file_path}")
        sys.exit(1)
    with open(file_path) as f:
        stations = [line.strip() for line in f if line.strip()]
    if not stations:
        logger.error("No station IDs found in stations file.")
        sys.exit(1)
    return stations

def parse_session_periods(df_metadata):
    """
    From DataFrame with columns 'id','session_start_time','session_stop_time',
    parse ISO8601 times into datetime.date, return dict station_id -> (start_date, stop_date).
    """
    station_periods = {}
    for _, row in df_metadata.iterrows():
        sid = row['id']
        s_start = row.get('session_start_time', None)
        s_stop = row.get('session_stop_time', None)
        # Parse ISO8601 if present
        if isinstance(s_start, str) and pd.notna(s_start):
            try:
                dt0 = datetime.fromisoformat(s_start.rstrip("Z"))
                start_d = dt0.date()
            except:
                start_d = None
        else:
            start_d = None
        if isinstance(s_stop, str) and pd.notna(s_stop):
            try:
                dt1 = datetime.fromisoformat(s_stop.rstrip("Z"))
                stop_d = dt1.date()
            except:
                stop_d = None
        else:
            stop_d = None
        station_periods[sid] = (start_d, stop_d)
    return station_periods

def generate_candidate_filenames(station: str, current_date: date):
    """
    Return a list of possible RINEX filenames to try for station/date.
    Includes lowercase and uppercase station codes.
    """
    st_low = station.lower()
    st_up = station.upper()
    year = current_date.year
    doy = current_date.timetuple().tm_yday
    yy = year % 100
    # pattern: <station><DOY>00.<yy>d.Z
    patterns = [
        f"{st_low}{doy:03d}00.{yy:02d}d.Z",
        f"{st_up}{doy:03d}00.{yy:02d}d.Z",
    ]
    # remove duplicates
    seen = set(); unique = []
    for p in patterns:
        if p not in seen:
            seen.add(p); unique.append(p)
    return unique

def generate_rinex_url_and_path(station: str, current_date: date, rinex_root: str):
    """
    Given station and date, generate list of (url, local_path) candidates.
    """
    year = current_date.year
    doy = current_date.timetuple().tm_yday
    base_dir = os.path.join(rinex_root, f"{year}", f"{doy:03d}")
    candidates = []
    for fname in generate_candidate_filenames(station, current_date):
        url = f"https://gage-data.earthscope.org/archive/gnss/rinex/obs/{year}/{doy:03d}/{fname}"
        local_path = os.path.join(base_dir, fname)
        candidates.append((url, local_path))
    return candidates

def ensure_directory(path: str):
    dirpath = os.path.dirname(path)
    if not os.path.exists(dirpath):
        try:
            os.makedirs(dirpath, exist_ok=True)
            logger.debug(f"Created directory: {dirpath}")
        except Exception as e:
            logger.error(f"Failed to create directory {dirpath}: {e}")
            raise

def download_file(url: str, local_path: str, token: str, max_retries=MAX_RETRIES) -> bool:
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
    }
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=HTTP_TIMEOUT) as resp:
                if resp.status_code == 200:
                    ensure_directory(local_path)
                    with open(local_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    logger.info(f"Downloaded: {url}")
                    return True
                elif resp.status_code == 404:
                    logger.debug(f"File not found (404): {url}")
                    return False
                else:
                    logger.warning(f"Unexpected status {resp.status_code} for URL: {url}")
                    if 500 <= resp.status_code < 600:
                        raise requests.HTTPError(f"Server error {resp.status_code}")
                    else:
                        return False
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {url}: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying after {RETRY_BACKOFF_SEC} seconds...")
                import time; time.sleep(RETRY_BACKOFF_SEC)
            else:
                logger.error(f"All {MAX_RETRIES} attempts failed for {url}")
                return False

def download_for_date_station(station: str, current_date: date, token: str, rinex_root: str, dry_run=False):
    """
    Attempt to download RINEX for station/date, trying multiple filename patterns.
    Returns (local_path, success_flag).
    """
    candidates = generate_rinex_url_and_path(station, current_date, rinex_root)
    if dry_run:
        for url, local_path in candidates:
            logger.info(f"[DRY RUN] Would try: {url}")
        return candidates[0][1], True

    for url, local_path in candidates:
        if os.path.isfile(local_path) and os.path.getsize(local_path) > 0:
            logger.debug(f"Already exists, skipping: {local_path}")
            return local_path, True
        success = download_file(url, local_path, token)
        if success:
            return local_path, True
    logger.debug(f"All filename variants failed for station {station} date {current_date}")
    return candidates[0][1], False

def daterange(start_date: date, end_date: date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def download_rinex_batch(stations, start_date: date, end_date: date, out_root: str,
                         station_periods: dict,
                         max_workers=MAX_WORKERS_DEFAULT, dry_run=False):
    """
    Download RINEX files for stations over date range, limited by each station's operational period.
    """
    token = get_bearer_token()
    tasks = []
    for st in stations:
        st_start, st_stop = station_periods.get(st, (None, None))
        eff_start = start_date
        if st_start and st_start > eff_start:
            eff_start = st_start
        eff_end = end_date
        if st_stop and st_stop < eff_end:
            eff_end = st_stop
        if eff_end < eff_start:
            logger.info(f"Skipping station {st}: no overlap with date range")
            continue
        for single_date in daterange(eff_start, eff_end):
            tasks.append((st, single_date))
    total = len(tasks)
    logger.info(f"Starting download: {len(stations)} stations, {total} station-days, out_root={out_root}, workers={max_workers}")

    results = []
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(download_for_date_station, st, dt, token, out_root, dry_run): (st, dt)
            for (st, dt) in tasks
        }
        for future in as_completed(future_to_task):
            st, dt = future_to_task[future]
            try:
                local_path, success = future.result()
                if not success:
                    logger.debug(f"Failed: station {st}, date {dt.isoformat()}")
                with lock:
                    results.append((st, dt, local_path, success))
            except Exception as e:
                logger.error(f"Exception for station {st}, date {dt}: {e}")
                with lock:
                    results.append((st, dt, None, False))
    n_success = sum(1 for r in results if r[3])
    n_fail = total - n_success
    logger.info(f"Download complete: {n_success} succeeded, {n_fail} failed or missing")
    return results

# ========== Main ==========
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unified GNSS RINEX Downloader")
    parser.add_argument("--min-lat", type=float, required=True, help="Minimum latitude of bounding box")
    parser.add_argument("--max-lat", type=float, required=True, help="Maximum latitude of bounding box")
    parser.add_argument("--min-lon", type=float, required=True, help="Minimum longitude of bounding box")
    parser.add_argument("--max-lon", type=float, required=True, help="Maximum longitude of bounding box")
    parser.add_argument("--stations-file", type=str,
                        help="Optional: text file with station IDs (one per line) to restrict download")
    parser.add_argument("--start-date", type=str, required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--out-root", type=str, default="./rinex_data", help="Root directory for RINEX files")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS_DEFAULT, help="Concurrent downloads")
    parser.add_argument("--dry-run", action="store_true", help="Dry run; only log URLs")
    args = parser.parse_args()

    # Parse date range
    try:
        y0, m0, d0 = map(int, args.start_date.split("-"))
        ye, me, de = map(int, args.end_date.split("-"))
        start_dt = date(y0, m0, d0)
        end_dt = date(ye, me, de)
    except Exception as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)
    if end_dt < start_dt:
        logger.error("End date must be on or after start date.")
        sys.exit(1)

    # Query metadata for bounding box
    logger.info(f"Querying station metadata for bbox lat[{args.min_lat},{args.max_lat}], lon[{args.min_lon},{args.max_lon}]")
    df_meta = query_metadata_bbox(args.min_lat, args.max_lat, args.min_lon, args.max_lon)
    if df_meta.empty:
        logger.error("No stations found in bounding box.")
        sys.exit(1)
    #logger.info(f"Found {len(df_meta)} stations in bounding box.")

    # Parse station periods
    station_periods = parse_session_periods(df_meta)

    # Determine station list
    if args.stations_file:
        stations_requested = load_station_list(args.stations_file)
        stations = [st for st in stations_requested if st in station_periods]
        missing = [st for st in stations_requested if st not in station_periods]
        if missing:
            logger.warning(f"Stations in file not found in metadata bbox query: {missing}")
        if not stations:
            logger.error("No valid stations to download after filtering.")
            sys.exit(1)
    else:
        stations = list(station_periods.keys())

    # Run download batch
    download_rinex_batch(stations, start_dt, end_dt, out_root=args.out_root,
                         station_periods=station_periods,
                         max_workers=args.workers, dry_run=args.dry_run)