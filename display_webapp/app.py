from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify, request
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import time
from dotenv import load_dotenv
import requests
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError, ResourceNotFoundError

# Load environment variables from .env in project root if present
root_env = Path(__file__).resolve().parent.parent / ".env"
if root_env.exists():
    load_dotenv(dotenv_path=root_env)
else:
    load_dotenv()

app = Flask(__name__)

# Keep in sync with SPIKE_THRESHOLD in ingestion_function/ingestion.py
SPIKE_THRESHOLD = 35.0
EVENT_WINDOW_DAYS = 7
READING_INTERVAL_MINUTES = 5
SPIKE_EVENTS_TABLE = "SpikeEvents"
# How far back the dashboard looks for each sensor's latest reading
READING_WINDOWS_HOURS = (1, 6, 24, 48, 168)
# A sensor that hasn't measured anything for this long is shown as offline
OFFLINE_AFTER = timedelta(hours=2)

# Live test sensor: recording state and readings share one table
LIVE_TABLE = "LiveSensor"
LIVE_SESSION_PARTITION = "session"
LIVE_SESSION_ROW = "current"
LIVE_FIELDS = "pm2.5_atm_a,pm2.5_atm_b,temperature,humidity,last_seen"
LIVE_MAX_MINUTES = 120
LIVE_REPORT_SECONDS = 120
LIVE_RETRY_SECONDS = 60

# Multi-process caching across Gunicorn workers (L1 memory + L2 shared temp disk)
DASHBOARD_CACHE_SECONDS = 180
EVENTS_CACHE_SECONDS = 300
_dashboard_cache = {"expires_at": 0, "data": None}
_events_cache = {"expires_at": 0, "data": None}
CACHE_DIR = Path(tempfile.gettempdir()) / "healthygulf_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _get_cache(cache_name, mem_cache):
    if app.testing or app.config.get("TESTING") or "unittest" in sys.modules:
        return None
    now_ts = time.time()
    if mem_cache["data"] and mem_cache["expires_at"] > now_ts:
        return mem_cache["data"]
    # Shared across Gunicorn workers
    cache_file = CACHE_DIR / f"{cache_name}.json"
    if cache_file.is_file():
        try:
            payload = json.loads(cache_file.read_text(encoding="utf-8"))
            if payload.get("expires_at", 0) > now_ts:
                data = payload.get("data")
                mem_cache["data"] = data
                mem_cache["expires_at"] = payload["expires_at"]
                return data
        except Exception:
            pass
    return None


def _set_cache(cache_name, mem_cache, data, ttl_seconds):
    if app.testing or app.config.get("TESTING") or "unittest" in sys.modules:
        return
    expires_at = time.time() + ttl_seconds
    mem_cache["data"] = data
    mem_cache["expires_at"] = expires_at
    cache_file = CACHE_DIR / f"{cache_name}.json"
    temp_file = CACHE_DIR / f"{cache_name}.json.tmp.{os.getpid()}"
    try:
        temp_file.write_text(json.dumps({"expires_at": expires_at, "data": data}, default=str), encoding="utf-8")
        temp_file.replace(cache_file)
    except Exception:
        pass

# US EPA PM2.5 AQI breakpoints (2024 revision):
# (pm_low, pm_high, aqi_low, aqi_high, category, pin color, text color)
AQI_BREAKPOINTS = [
    (0.0, 9.0, 0, 50, "Good", "#00e400", "#1a1a1a"),
    (9.1, 35.4, 51, 100, "Moderate", "#ffff00", "#1a1a1a"),
    (35.5, 55.4, 101, 150, "Unhealthy for Sensitive Groups", "#ff7e00", "#1a1a1a"),
    (55.5, 125.4, 151, 200, "Unhealthy", "#ff0000", "#ffffff"),
    (125.5, 225.4, 201, 300, "Very Unhealthy", "#8f3f97", "#ffffff"),
    (225.5, 325.4, 301, 500, "Hazardous", "#7e0023", "#ffffff"),
]
NO_DATA = {"aqi": None, "category": "No data", "color": "#9e9e9e", "text_color": "#ffffff"}


def pm25_to_aqi(pm25):
    """Convert a PM2.5 concentration (µg/m³) to a US EPA AQI value + display info."""
    if pm25 is None:
        return dict(NO_DATA)
    # EPA truncates PM2.5 to one decimal place before looking up the breakpoint
    pm = max(0.0, math.floor(float(pm25) * 10) / 10)
    for pm_lo, pm_hi, aqi_lo, aqi_hi, category, color, text_color in AQI_BREAKPOINTS:
        if pm <= pm_hi:
            aqi = round((aqi_hi - aqi_lo) / (pm_hi - pm_lo) * (pm - pm_lo) + aqi_lo)
            return {"aqi": aqi, "category": category, "color": color, "text_color": text_color}
    # Beyond the top of the scale
    _, _, _, _, category, color, text_color = AQI_BREAKPOINTS[-1]
    return {"aqi": 500, "category": category, "color": color, "text_color": text_color}


def _parse_utc(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _first_float(*values):
    """Return the first value that can be converted to float, else None."""
    for value in values:
        if value is None or value == "":
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _event_from_entity(entity, sensor):
    start = datetime.fromisoformat(str(entity["start_time"]))
    end = datetime.fromisoformat(str(entity["end_time"]))
    count = int(entity.get("reading_count") or 1)
    return {
        "sensor_id": str(entity["PartitionKey"]),
        "sensor_name": sensor.get("Name") or entity.get("sensor_name") or f"Sensor {entity['PartitionKey']}",
        "location": sensor.get("Location") or "",
        "start": start.isoformat(),
        "end": end.isoformat(),
        # Readings arrive about every 5 minutes, so each one covers roughly that long
        "duration_minutes": max(READING_INTERVAL_MINUTES, int((end - start).total_seconds() // 60) + READING_INTERVAL_MINUTES),
        "peak_pm2_5": float(entity["peak_pm2_5"]),
        "mean_pm2_5": float(entity["total_pm2_5"]) / count,
        "reading_count": count,
    }


def load_spike_events(force_refresh=False):
    """Read spike events that the ingestion function records as readings arrive."""
    if not force_refresh:
        cached = _get_cache("events", _events_cache)
        if cached is not None:
            return cached

    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")

    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    try:
        registry = {str(entity["RowKey"]): entity for entity in service.get_table_client("SensorRegistry").list_entities()}
    except ResourceNotFoundError:
        registry = {}

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=EVENT_WINDOW_DAYS)
    try:
        rows = list(service.get_table_client(SPIKE_EVENTS_TABLE).query_entities(
            query_filter="end_time ge @start_time",
            parameters={"start_time": start_time.isoformat()},
        ))
    except ResourceNotFoundError:
        rows = []

    events = [_event_from_entity(row, registry.get(str(row["PartitionKey"]), {})) for row in rows]
    events.sort(key=lambda event: event["start"], reverse=True)
    result = {
        "events": events,
        "sensor_count": sum(1 for sensor in registry.values() if sensor.get("IsActive")),
        "failed_sensor_count": 0,
        "errors": [],
        "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(),
        "fetched_at": end_time.isoformat(),
        "threshold": SPIKE_THRESHOLD,
        "history_ready": True,
        "message": "",
    }
    _set_cache("events", _events_cache, result, EVENTS_CACHE_SECONDS)
    return result


def load_dashboard_data(force_refresh=False):
    """Read the registry + readings from Azure Table Storage and shape them for the page."""
    if not force_refresh:
        cached = _get_cache("dashboard", _dashboard_cache)
        if cached is not None:
            return cached

    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")

    service = TableServiceClient.from_connection_string(conn_str=connection_string)

    # 1. Sensor metadata (name, location, active flag, coordinates)
    registry = {}
    try:
        for entity in service.get_table_client("SensorRegistry").list_entities():
            registry[str(entity["RowKey"])] = entity
    except ResourceNotFoundError:
        pass

    # 2. Recent readings, queried per sensor by key range (PartitionKey = sensor, RowKey starts
    # with the save time). Filtering on a non-key property would scan the whole table.
    table = service.get_table_client("PurpleAirData")

    def _recent_readings(sensor_id):
        # If ingestion has paused, widen the window step by step
        for hours in READING_WINDOWS_HOURS:
            since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y%m%d%H%M%S")
            rows = list(table.query_entities(
                query_filter="PartitionKey eq @sensor_id and RowKey ge @since",
                parameters={"sensor_id": sensor_id, "since": since},
            ))
            if rows:
                return rows
        return []

    try:
        with ThreadPoolExecutor(max_workers=min(25, max(len(registry), 1))) as executor:
            readings = [row for rows in executor.map(_recent_readings, list(registry)) for row in rows]
    except ResourceNotFoundError:
        readings = []
    readings.sort(key=lambda r: r.get("timestamp") or "", reverse=True)

    latest_by_sensor = {}
    for reading in readings:
        latest_by_sensor.setdefault(str(reading["PartitionKey"]), reading)

    # 3. One entry per sensor: registry metadata + its most recent reading
    now = datetime.now(timezone.utc)
    sensors = []
    for sensor_id in sorted(set(registry) | set(latest_by_sensor)):
        reg = registry.get(sensor_id, {})
        latest = latest_by_sensor.get(sensor_id, {})
        pm25 = _first_float(latest.get("pm2_5"))
        # When the sensor itself last measured; an offline sensor keeps repeating an old value
        measured_at = latest.get("last_seen") or latest.get("timestamp")

        sensor = {
            "id": sensor_id,
            "name": reg.get("Name") or latest.get("name") or f"Sensor {sensor_id}",
            "location": reg.get("Location") or "",
            "is_active": bool(reg.get("IsActive", False)),
            "is_online": bool(measured_at) and now - _parse_utc(measured_at) <= OFFLINE_AFTER,
            # Registry coordinates win; fall back to coordinates stored on the latest reading
            "latitude": _first_float(reg.get("Latitude"), latest.get("latitude")),
            "longitude": _first_float(reg.get("Longitude"), latest.get("longitude")),
            "pm2_5": pm25,
            "temperature": _first_float(latest.get("temperature")),
            "humidity": _first_float(latest.get("humidity")),
            "timestamp": measured_at,
        }
        sensor.update(pm25_to_aqi(pm25))
        sensors.append(sensor)

    # Table order: online sensors first, worst air at the top
    sensors.sort(key=lambda s: (not s["is_online"], -(s["aqi"] if s["aqi"] is not None else -1), s["name"].lower()))

    # 4. Summary cards. Use online sensors when there are any, so stale values don't skew "right now"
    with_data = [s for s in sensors if s["pm2_5"] is not None]
    current = [s for s in with_data if s["is_online"]] or with_data
    avg_pm25 = sum(s["pm2_5"] for s in current) / len(current) if current else None
    timestamps = [s["timestamp"] for s in sensors if s["timestamp"]]
    summary = {
        "sensor_count": len(sensors),
        "online_count": sum(1 for s in sensors if s["is_online"]),
        "mapped_count": sum(1 for s in sensors if s["latitude"] is not None and s["longitude"] is not None),
        "avg_pm2_5": avg_pm25,
        "avg_aqi": pm25_to_aqi(avg_pm25),
        "worst": max(current, key=lambda s: s["pm2_5"]) if current else None,
        "last_updated": max(timestamps, key=_parse_utc) if timestamps else None,
    }

    result = {"sensors": sensors, "summary": summary}
    _set_cache("dashboard", _dashboard_cache, result, DASHBOARD_CACHE_SECONDS)
    return result


@app.route('/')
def index():
    try:
        force = request.args.get("refresh") == "1"
        data = load_dashboard_data(force_refresh=force)
    except Exception as e:
        return f"Error fetching data from Azure: {str(e)}", 500
    return render_template('index.html', **data, aqi_scale=AQI_BREAKPOINTS)


@app.route('/api/sensors')
def api_sensors():
    """JSON version of the per-sensor data (handy for debugging or future front-ends)."""
    try:
        force = request.args.get("refresh") == "1"
        data = load_dashboard_data(force_refresh=force)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"sensors": data["sensors"], "summary": data["summary"]})


@app.route('/api/events')
def api_events():
    try:
        force = request.args.get("refresh") == "1"
        data = load_spike_events(force_refresh=force)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 503
    except Exception as exc:
        app.logger.exception("Unable to load PurpleAir spike history")
        return jsonify({"error": str(exc)}), 500
    return jsonify(data)


def _live_config():
    api_key = os.environ.get("PURPLEAIR_API_KEY")
    sensor_index = os.environ.get("LIVE_SENSOR_INDEX")
    if not api_key or not sensor_index:
        return None
    return {"api_key": api_key, "sensor_index": sensor_index, "read_key": os.environ.get("LIVE_SENSOR_READ_KEY")}


def _live_table():
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")
    return TableServiceClient.from_connection_string(conn_str=connection_string).get_table_client(LIVE_TABLE)


def _live_session(table):
    try:
        return table.get_entity(partition_key=LIVE_SESSION_PARTITION, row_key=LIVE_SESSION_ROW)
    except ResourceNotFoundError:
        return None


def _live_is_recording(session, now):
    return bool(session) and session.get("status") == "recording" and now < session["ends_at"]


def _live_collect(table, session, config, now):
    """Fetch the sensor's next report if one is due, and return the session's updated fields.

    Called while someone has the page open, so nothing is spent otherwise. The etag
    claim means only one Gunicorn worker calls PurpleAir, however many people are watching.
    """
    if now < session.get("next_fetch_at", 0):
        return {}
    claim = dict(session)
    claim["next_fetch_at"] = int(now) + LIVE_RETRY_SECONDS
    try:
        table.update_entity(claim, mode=UpdateMode.REPLACE, etag=session.metadata["etag"], match_condition=MatchConditions.IfNotModified)
    except ResourceModifiedError:
        return {}

    params = {"fields": LIVE_FIELDS}
    if config["read_key"]:
        params["read_key"] = config["read_key"]
    updates = {"error": "", "next_fetch_at": claim["next_fetch_at"]}
    try:
        response = requests.get(
            f"https://api.purpleair.com/v1/sensors/{config['sensor_index']}",
            headers={"X-API-Key": config["api_key"]},
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        sensor = response.json()["sensor"]
        last_seen = int(sensor["last_seen"])
        pm_a = _first_float(sensor.get("pm2.5_atm_a"))
        pm_b = _first_float(sensor.get("pm2.5_atm_b"))
        channels = [value for value in (pm_a, pm_b) if value is not None]
        if channels and last_seen != session.get("last_seen"):
            reading = {
                "PartitionKey": session["session_id"],
                "RowKey": f"{last_seen:012d}",
                "pm": round(sum(channels) / len(channels), 2),
                "pm_a": pm_a,
                "pm_b": pm_b,
                "temperature": _first_float(sensor.get("temperature")),
                "humidity": _first_float(sensor.get("humidity")),
            }
            table.upsert_entity({k: v for k, v in reading.items() if v is not None})
            updates["last_seen"] = last_seen
        # A new report is due about 2 minutes after the last one, so wait until then to ask again
        updates["next_fetch_at"] = int(max(last_seen + LIVE_REPORT_SECONDS + 5, now + LIVE_RETRY_SECONDS))
    except requests.RequestException as exc:
        # Only the status code: the exception text includes the request URL and read key
        status = getattr(exc.response, "status_code", None)
        updates["error"] = f"PurpleAir returned an error ({status})." if status else "Couldn't reach PurpleAir."
    except (KeyError, TypeError, ValueError):
        updates["error"] = "PurpleAir sent a response that couldn't be read."
    table.update_entity({"PartitionKey": LIVE_SESSION_PARTITION, "RowKey": LIVE_SESSION_ROW, **updates}, mode=UpdateMode.MERGE)
    return updates


def _live_sample(entity):
    sample = {
        "t": int(entity["RowKey"]) * 1000,
        "pm": entity["pm"],
        "pm_a": entity.get("pm_a"),
        "pm_b": entity.get("pm_b"),
        "temperature": entity.get("temperature"),
        "humidity": entity.get("humidity"),
    }
    aqi = pm25_to_aqi(entity["pm"])
    sample.update(aqi=aqi["aqi"], category=aqi["category"], color=aqi["color"], text_color=aqi["text_color"])
    return sample


def live_state(after=0):
    """The page's view of the recording. Also collects a new report when one is due."""
    config = _live_config()
    now = time.time()
    state = {"configured": bool(config), "recording": False, "session_id": None, "ends_at": None, "error": "",
             "samples": [], "server_time": int(now * 1000), "report_seconds": LIVE_REPORT_SECONDS}
    if not config:
        return state
    table = _live_table()
    session = _live_session(table)
    if not session:
        return state
    state["recording"] = _live_is_recording(session, now)
    state["session_id"] = session["session_id"]
    state["ends_at"] = session["ends_at"] * 1000
    state["error"] = session.get("error", "")
    if state["recording"]:
        state["error"] = _live_collect(table, session, config, now).get("error", state["error"])
    rows = table.query_entities(
        query_filter="PartitionKey eq @session_id and RowKey gt @after",
        parameters={"session_id": session["session_id"], "after": f"{after:012d}"},
    )
    state["samples"] = sorted((_live_sample(row) for row in rows), key=lambda sample: sample["t"])
    return state


def live_start():
    table = _live_table()
    try:
        table.create_table()
    except ResourceExistsError:
        pass
    now = time.time()
    # Starting while a recording is running just joins it
    if not _live_is_recording(_live_session(table), now):
        table.upsert_entity({
            "PartitionKey": LIVE_SESSION_PARTITION,
            "RowKey": LIVE_SESSION_ROW,
            "session_id": datetime.fromtimestamp(now, timezone.utc).strftime("%Y%m%d%H%M%S"),
            "status": "recording",
            "ends_at": int(now) + LIVE_MAX_MINUTES * 60,
            "next_fetch_at": 0,
            "error": "",
        }, mode=UpdateMode.REPLACE)
    return live_state()


def live_stop():
    table = _live_table()
    if _live_session(table):
        table.update_entity({"PartitionKey": LIVE_SESSION_PARTITION, "RowKey": LIVE_SESSION_ROW, "status": "stopped"}, mode=UpdateMode.MERGE)
    return live_state()


@app.route('/api/live')
def api_live():
    return _live_response(lambda: live_state(request.args.get("after", 0, type=int)))


@app.route('/api/live/start', methods=['POST'])
def api_live_start():
    if not _live_config():
        return jsonify({"error": "The live sensor isn't set up on this server."}), 503
    return _live_response(live_start)


@app.route('/api/live/stop', methods=['POST'])
def api_live_stop():
    return _live_response(live_stop)


def _live_response(load):
    try:
        return jsonify(load())
    except Exception:
        app.logger.exception("Live sensor request failed")
        return jsonify({"error": "Live sensor data is unavailable right now."}), 500


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting dashboard at http://localhost:{port}...")
    app.run(host='0.0.0.0', port=port, debug=True)
