#!/usr/bin/env python3
"""
live_tracker.py

Refactored aircraft tracker:

- Polls dump1090 every 5 seconds (configurable).
- For each aircraft with coordinates, saves a timestamped datapoint into a per-hex JSON file under tracked_dir/<hex>.json
  Format: { "<ISO timestamp>": { ... observation ... }, ... }
- Caches hexdb.io metadata to reduce calls.
- Every CLEANUP_INTERVAL seconds (default 1800 = 30 min):
    * For each tracked hex file, if last update is older than CLEANUP_AGE (30 min),
      find the datapoint with MAX distance and build a final "row" using that datapoint
      (fill missing flight info from any other datapoints if available), insert into SQLite table,
      then delete the per-hex file.
- SQLite schema created automatically if missing.
- Safe-ish error handling: errors logged to errors.log.
"""

import json
import time
import datetime
import math
import os
import sys
import traceback
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- CONFIG ---------------- #
CONFIG = {
    "dump1090_host": None,  # if None it'll read from private/private.json key 'url_ip'
    "private_json": "private/private.json",
    "tracked_dir": "tracked_hexes",         # per-hex json files stored here
    "aircraft_dict_file": "aircraft_dictionary.json",  # optional cache of known aircraft (read/write)
    "sqlite_db": "flights.db",
    "poll_interval": 5,                     # seconds between dump1090 polls
    "cleanup_interval": 1800,               # seconds between cleanup runs (default 30 min)
    "cleanup_age": 1800,                    # seconds: only extract hexcodes with no update for this long
    "hexdb_base_url": "https://hexdb.io/api/v1/aircraft",
    "hexdb_timeout": 5,
    "hexdb_cache_ttl": 24 * 3600,           # seconds to keep hexdb metadata in cache
    "log_file": "tracker.log",
    "error_log": "errors.log",
}
# ---------------------------------------- #

# Ensure directories exist
Path(CONFIG["tracked_dir"]).mkdir(parents=True, exist_ok=True)
Path(os.path.dirname(CONFIG["aircraft_dict_file"]) or ".").mkdir(parents=True, exist_ok=True)

# HTTP session with retries
def create_requests_session(retries=3, backoff_factor=0.3, status_forcelist=(500,502,503,504)):
    s = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    return s

SESSION = create_requests_session()

# Logging helpers
def log(msg: str):
    ts = datetime.datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}", flush=True)
    with open(CONFIG["log_file"], "a") as f:
        f.write(f"[{ts}] {msg}\n")

def log_error(msg: str):
    ts = datetime.datetime.utcnow().isoformat()
    print(f"[{ts}] ERROR: {msg}", file=sys.stderr, flush=True)
    with open(CONFIG["error_log"], "a") as f:
        f.write(f"[{ts}] ERROR: {msg}\n")

# Simple haversine (returns km)
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    # calculate carefully: convert to radians
    lat1r = math.radians(lat1)
    lon1r = math.radians(lon1)
    lat2r = math.radians(lat2)
    lon2r = math.radians(lon2)
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = math.sin(dlat/2.0)**2 + math.cos(lat1r)*math.cos(lat2r)*(math.sin(dlon/2.0)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    R = 6371.0  # km
    return round(R * c, 3)

# Load private private (home coords, dump1090 IP)
def load_private():
    p = Path(CONFIG["private_json"])
    if not p.exists():
        raise FileNotFoundError(f"{p} not found; create with keys: url_ip, home_lat, home_lon")
    data = json.loads(p.read_text())
    return data

# SQLite helpers
import sqlite3

def init_db(db_path):
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS flights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hex TEXT NOT NULL,
        airline TEXT,
        registration TEXT,
        aircraft TEXT,
        aircraft_icao TEXT,
        max_distance_km REAL,
        latitude REAL,
        longitude REAL,
        altitude INTEGER,
        speed REAL,
        vert_rate REAL,
        track REAL,
        first_seen_time TEXT,
        last_seen_time TEXT,
        sample_time_of_max TEXT,
        inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,
        json_blob TEXT
    );
    """)
    conn.commit()
    return conn

# Per-hex file helpers
def hex_file_path(hexcode: str) -> Path:
    return Path(CONFIG["tracked_dir"]) / f"{hexcode.lower()}.json"

def load_hex_data(hexcode: str) -> Dict[str, Dict]:
    p = hex_file_path(hexcode)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        log_error(f"Failed to load hex file {p}; resetting file.")
        p.unlink(missing_ok=True)
        return {}

def save_hex_data(hexcode: str, data: Dict[str, Dict]):
    p = hex_file_path(hexcode)
    p.write_text(json.dumps(data, indent=2))

# Hexdb cache: in-memory + persisted to aircraft_dictionary.json
HEXDB_CACHE: Dict[str, Tuple[dict, float]] = {}  # hex -> (metadata dict, fetched_at_epoch)
_aircraft_dict_loaded = False
def load_aircraft_dictionary():
    global _aircraft_dict_loaded
    path = Path(CONFIG["aircraft_dict_file"])
    if path.exists():
        try:
            d = json.loads(path.read_text())
            # shape into HEXDB_CACHE entries
            now = time.time()
            for k,v in d.items():
                HEXDB_CACHE[k] = (v, now)  # treat as freshly loaded
            _aircraft_dict_loaded = True
            log("Loaded aircraft_dictionary cache.")
        except Exception as e:
            log_error(f"Failed to load aircraft_dictionary.json: {e}")
    else:
        # create empty file
        path.write_text(json.dumps({}))
        _aircraft_dict_loaded = True

def persist_aircraft_dictionary():
    path = Path(CONFIG["aircraft_dict_file"])
    serial = {k: v[0] for k, v in HEXDB_CACHE.items()}
    path.write_text(json.dumps(serial, indent=2))

def get_hexdb_metadata(hexcode: str) -> dict:
    """Return metadata for hexcode, using in-memory cache and persisted cache on disk."""
    hexcode = hexcode.lower()
    now = time.time()
    # check cache
    if hexcode in HEXDB_CACHE:
        meta, fetched_at = HEXDB_CACHE[hexcode]
        if (now - fetched_at) < CONFIG["hexdb_cache_ttl"]:
            return meta
    # fetch from API
    try:
        url = f"{CONFIG['hexdb_base_url']}/{hexcode}"
        r = SESSION.get(url, timeout=CONFIG["hexdb_timeout"])
        if r.status_code == 200:
            meta = r.json()
        else:
            meta = {}
            log_error(f"hexdb returned status {r.status_code} for {hexcode}")
    except Exception as e:
        log_error(f"hexdb fetch failed for {hexcode}: {e}")
        meta = {}
    HEXDB_CACHE[hexcode] = (meta, now)
    # persist cache to disk (best-effort)
    try:
        persist_aircraft_dictionary()
    except Exception as e:
        log_error(f"Failed to persist aircraft dictionary: {e}")
    return meta

# Build an observation object from dump1090 plane entry
def build_observation(plane: dict, home_lat: float, home_lon: float) -> Dict[str, Any]:
    # plane is one element of dump1090's 'aircraft' list
    hexcode = plane.get("hex", "").lower()
    now_utc = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    obs = {}
    obs["hex"] = hexcode
    obs["seen_pos"] = plane.get("seen_pos", None)
    obs["lat"] = plane.get("lat", None)
    obs["lon"] = plane.get("lon", None)
    obs["altitude"] = plane.get("altitude", None)
    obs["speed"] = plane.get("speed", None)
    obs["vert_rate"] = plane.get("vert_rate", None)
    obs["track"] = plane.get("track", None)
    # flight field might be present
    obs["flight"] = (plane.get("flight") or "").strip()
    # hexdb metadata (only identifiers will be filled here; heavy fields come from get_hexdb_metadata)
    # compute distance if we have coords
    if obs["lat"] is not None and obs["lon"] is not None:
        obs["distance_km"] = haversine_km(home_lat, home_lon, obs["lat"], obs["lon"])
    else:
        obs["distance_km"] = None
    obs["timestamp"] = now_utc
    return obs

# select best datapoint for a hex: datapoint with max 'distance_km'
def select_max_distance_datapoint(data: Dict[str, Dict]) -> Tuple[Optional[str], Optional[Dict]]:
    if not data:
        return None, None
    # data keys are timestamps; values are observation dicts
    best_ts = None
    best_obs = None
    best_dist = -1.0
    for ts, obs in data.items():
        d = obs.get("distance_km")
        if d is None:
            continue
        # use strictly greater to prefer later higher values if equal
        if d > best_dist:
            best_dist = d
            best_ts = ts
            best_obs = obs
    # fallback if none had distance (pick latest)
    if best_obs is None:
        # pick latest timestamp
        try:
            best_ts = max(data.keys())
            best_obs = data[best_ts]
        except Exception:
            return None, None
    return best_ts, best_obs

# Merge flight info from other timestamps if missing from selected datapoint
def ensure_flight_info(best_obs: Dict, all_obs: Dict) -> Dict:
    # fields of interest: flight, possibly airline/registration/aircraft from hexdb
    if best_obs.get("flight"):
        return best_obs
    # try to find any other obs with non-empty flight
    for ts, obs in all_obs.items():
        if obs.get("flight"):
            best_obs["flight"] = obs["flight"]
            return best_obs
    return best_obs

# Convert observation + hexdb metadata into a DB row dict
def build_db_row(hexcode: str, sample_ts: str, sample_obs: Dict, first_seen_ts: str, last_seen_ts: str) -> Dict:
    row = {}
    row["hex"] = hexcode
    # hexdb metadata (attempt to pull common attributes)
    meta = get_hexdb_metadata(hexcode)
    # map keys carefully; hexdb API responses vary; use safe get with defaults
    airline = meta.get("RegisteredOwners") or meta.get("owner") or meta.get("airline") or ""
    registration = meta.get("Registration") or meta.get("registration") or ""
    aircraft = meta.get("Type") or meta.get("TypeName") or meta.get("type") or ""
    icao = meta.get("ICAOTypeCode") or meta.get("icao") or ""
    row.update({
        "airline": airline,
        "registration": registration,
        "aircraft": aircraft,
        "aircraft_icao": icao,
        "max_distance_km": sample_obs.get("distance_km"),
        "latitude": sample_obs.get("lat"),
        "longitude": sample_obs.get("lon"),
        "altitude": sample_obs.get("altitude"),
        "speed": sample_obs.get("speed"),
        "vert_rate": sample_obs.get("vert_rate"),
        "track": sample_obs.get("track"),
        "first_seen_time": first_seen_ts,
        "last_seen_time": last_seen_ts,
        "sample_time_of_max": sample_ts,
        "json_blob": json.dumps(sample_obs),
    })
    # if flight missing in meta, attempt to take from sample_obs or other obs (handled before)
    # If hexdb meta contains better identifiers we keep them; otherwise user might want 'flight' too - can be extended
    return row

def insert_row_into_db(conn: sqlite3.Connection, row: Dict):
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO flights (hex, airline, registration, aircraft, aircraft_icao,
                         max_distance_km, latitude, longitude, altitude, speed, vert_rate, track,
                         first_seen_time, last_seen_time, sample_time_of_max, json_blob)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        row.get("hex"),
        row.get("airline"),
        row.get("registration"),
        row.get("aircraft"),
        row.get("aircraft_icao"),
        row.get("max_distance_km"),
        row.get("latitude"),
        row.get("longitude"),
        row.get("altitude"),
        row.get("speed"),
        row.get("vert_rate"),
        row.get("track"),
        row.get("first_seen_time"),
        row.get("last_seen_time"),
        row.get("sample_time_of_max"),
        row.get("json_blob"),
    ))
    conn.commit()

# Main routine
def main():
    try:
        private = load_private()
    except Exception as e:
        log_error(f"Private private load failed: {e}")
        raise

    dump_ip = CONFIG["dump1090_host"] or private.get("url_ip")
    if not dump_ip:
        raise ValueError("You must supply dump1090 IP in private/private.json as 'url_ip' or via CONFIG")
    dump_url = f"http://{dump_ip}/dump1090/data/aircraft.json"

    home_lat = float(private.get("home_lat"))
    home_lon = float(private.get("home_lon"))

    # init DB
    conn = init_db(CONFIG["sqlite_db"])

    # load aircraft dictionary cache if present
    load_aircraft_dictionary()

    last_cleanup = time.time()
    log(f"Starting live tracker: polling {dump_url} every {CONFIG['poll_interval']}s. Cleanup every {CONFIG['cleanup_interval']}s.")

    session = SESSION

    while True:
        cycle_start = time.time()
        try:
            r = session.get(dump_url, timeout=10)
            if r.status_code != 200:
                log_error(f"Non-200 from dump1090: {r.status_code}")
                time.sleep(CONFIG["poll_interval"])
                continue
            data = r.json()
            airborne = data.get("aircraft", [])
            log(f"On radar: {len(airborne)} aircraft")
            # process each plane
            for plane in airborne:
                try:
                    hexcode = (plane.get("hex") or "").strip().lower()
                    if not hexcode:
                        continue
                    # only process entries with seen_pos and coords
                    if 'seen_pos' not in plane:
                        continue
                    if plane.get("seen_pos") is None:
                        continue
                    # we'll accept sightings even if seen_pos slightly greater than 60, but you can enforce <60
                    if plane.get("seen_pos") > 300:
                        # stale position, skip
                        continue
                    if plane.get("lat") is None or plane.get("lon") is None:
                        continue

                    obs = build_observation(plane, home_lat, home_lon)

                    # Load hex file and append obs
                    hd = load_hex_data(hexcode)
                    # store keyed by ISO timestamp
                    ts = obs["timestamp"]
                    # avoid collisions: if timestamp exists, append :<n> suffix
                    store_key = ts
                    attempt = 1
                    while store_key in hd:
                        attempt += 1
                        store_key = f"{ts}-{attempt}"
                    hd[store_key] = obs
                    save_hex_data(hexcode, hd)
                except Exception as e:
                    log_error(f"Error processing plane {plane.get('hex')}: {e}\n")
            # Maybe do some periodic diagnostics
            # Cleanup pass every CONFIG['cleanup_interval'] seconds
            now = time.time()
            if (now - last_cleanup) >= CONFIG["cleanup_interval"]:
                log("Starting cleanup pass...")
                # iterate tracked files
                tracked_path = Path(CONFIG["tracked_dir"])
                for p in tracked_path.glob("*.json"):
                    hexcode = p.stem.lower()
                    try:
                        data_for_hex = load_hex_data(hexcode)
                        if not data_for_hex:
                            # no data - remove file
                            p.unlink(missing_ok=True)
                            continue
                        # find last update time
                        timestamps = sorted(data_for_hex.keys())
                        # parse ISO timestamps robustly
                        last_ts_str = timestamps[-1]
                        try:
                            last_ts = datetime.datetime.fromisoformat(last_ts_str.replace("Z", ""))
                        except Exception:
                            # fallback: use file mtime
                            last_ts = datetime.datetime.utcfromtimestamp(p.stat().st_mtime)
                        age_seconds = (datetime.datetime.utcnow() - last_ts).total_seconds()
                        if age_seconds < CONFIG["cleanup_age"]:
                            # not old enough
                            continue
                        # find first seen time and last seen time
                        first_seen = timestamps[0]
                        last_seen = timestamps[-1]
                        # pick datapoint with max distance
                        sample_ts, sample_obs = select_max_distance_datapoint(data_for_hex)
                        if sample_obs is None:
                            # shouldn't happen, remove and continue
                            log_error(f"No valid obs for {hexcode}, deleting file.")
                            p.unlink(missing_ok=True)
                            continue
                        # ensure flight info if missing by scanning others
                        sample_obs = ensure_flight_info(sample_obs, data_for_hex)
                        # build DB row
                        row = build_db_row(hexcode, sample_ts, sample_obs, first_seen, last_seen)
                        # insert
                        insert_row_into_db(conn, row)
                        log(f"Inserted {hexcode} into DB (sample {sample_ts}, distance {row.get('max_distance_km')} km). Removing hex file.")
                        # delete file
                        p.unlink(missing_ok=True)
                        # also remove from in-memory cache if present
                        HEXDB_CACHE.pop(hexcode, None)
                    except Exception as e:
                        log_error(f"Error during cleanup for {hexcode}: {e}\n")
                last_cleanup = now
                # persist aircraft_dictionary just in case
                try:
                    persist_aircraft_dictionary()
                except Exception as e:
                    log_error(f"Failed to persist aircraft dictionary on cleanup: {e}")

        except Exception as e:
            log_error(f"Main loop error: {e}\n")

        # sleep until next poll (account for time spent)
        elapsed = time.time() - cycle_start
        to_sleep = CONFIG["poll_interval"] - elapsed
        if to_sleep > 0:
            time.sleep(to_sleep)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user; exiting.")
    except Exception as e:
        log_error(f"Fatal error: {e}\n{traceback.format_exc()}")
        sys.exit(1)
