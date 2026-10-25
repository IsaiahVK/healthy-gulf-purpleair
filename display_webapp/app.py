from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify, request
import math
import os
from pathlib import Path
import time
from dotenv import load_dotenv
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError

# Load environment variables from .env in project root if present
root_env = Path(__file__).resolve().parent.parent / ".env"
if root_env.exists():
    load_dotenv(dotenv_path=root_env)
else:
    load_dotenv()

app = Flask(__name__)

SPIKE_THRESHOLD = 35.0
HISTORY_DAYS = 7
HISTORY_TABLE = "PurpleAirHistory"
HISTORY_STATE_TABLE = "PurpleAirHistorySyncState"

# In-memory caches to make page loads and tab switches instantaneous
DASHBOARD_CACHE_SECONDS = 45
EVENTS_CACHE_SECONDS = 300
_dashboard_cache = {"expires_at": 0, "data": None}
_events_cache = {"expires_at": 0, "data": None}

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


def detect_spike_events(sensor, readings, threshold=SPIKE_THRESHOLD):
    """Group consecutive 10-minute readings above the spike threshold."""
    sorted_readings = sorted(readings, key=lambda row: row["timestamp"])
    events = []
    current = []

    def finish_event():
        if not current:
            return
        values = [row["pm2_5"] for row in current]
        start = current[0]["timestamp"]
        end = current[-1]["timestamp"]
        events.append({
            "sensor_id": sensor["id"],
            "sensor_name": sensor["name"],
            "location": sensor.get("location", ""),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "duration_minutes": max(10, int((end - start).total_seconds() // 60) + 10),
            "peak_pm2_5": max(values),
            "mean_pm2_5": sum(values) / len(values),
            "reading_count": len(values),
        })

    for reading in sorted_readings:
        value = _first_float(reading.get("pm2_5"))
        if value is None or value <= threshold:
            finish_event()
            current = []
            continue
        if current and reading["timestamp"] - current[-1]["timestamp"] > timedelta(minutes=20):
            finish_event()
            current = []
        current.append({"timestamp": reading["timestamp"], "pm2_5": value})

    finish_event()
    return events


def load_spike_events(force_refresh=False):
    now_ts = time.time()
    if not force_refresh and _events_cache["data"] and _events_cache["expires_at"] > now_ts:
        return _events_cache["data"]

    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")

    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    registry = service.get_table_client("SensorRegistry")
    sensors = [
        {
            "id": str(entity["RowKey"]),
            "name": entity.get("Name") or f"Sensor {entity['RowKey']}",
            "location": entity.get("Location") or "",
        }
        for entity in registry.list_entities()
    ]

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=HISTORY_DAYS, minutes=20)
    history = service.get_table_client(HISTORY_TABLE)
    state = service.get_table_client(HISTORY_STATE_TABLE)
    try:
        checkpoints = list(state.query_entities(
            query_filter="PartitionKey eq @partition_key",
            parameters={"partition_key": "PurpleAirHistory"},
        ))
    except ResourceNotFoundError:
        checkpoints = []
    synced_sensor_ids = {str(checkpoint["RowKey"]) for checkpoint in checkpoints}
    if not synced_sensor_ids:
        return {
            "events": [],
            "sensor_count": len(sensors),
            "failed_sensor_count": 0,
            "errors": [],
            "fetched_at": end_time.isoformat(),
            "threshold": SPIKE_THRESHOLD,
            "history_ready": False,
            "message": "No stored PurpleAir history yet. Enable the history sync in the Azure Function App and wait for its first run.",
        }

    # Parallelize sensor history queries across a thread pool instead of 22 sequential network hops
    def _fetch_sensor_events(sensor):
        if sensor["id"] not in synced_sensor_ids:
            return None, {
                "sensor_id": sensor["id"],
                "sensor_name": sensor["name"],
                "message": "No successful history sync checkpoint for this sensor.",
            }
        try:
            rows = history.query_entities(
                query_filter="PartitionKey eq @partition_key and timestamp ge @start_timestamp",
                parameters={
                    "partition_key": sensor["id"],
                    "start_timestamp": start_time.isoformat(),
                },
            )
            readings = []
            for row in rows:
                timestamp = row.get("timestamp")
                pm2_5 = _first_float(row.get("pm2_5"))
                if timestamp and pm2_5 is not None:
                    parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    readings.append({"timestamp": parsed, "pm2_5": pm2_5})
            return detect_spike_events(sensor, readings), None
        except ResourceNotFoundError:
            raise
        except (ValueError, KeyError, TypeError) as exc:
            return None, {"sensor_id": sensor["id"], "sensor_name": sensor["name"], "message": str(exc)[:200]}

    events = []
    errors = []
    try:
        with ThreadPoolExecutor(max_workers=min(10, len(sensors) or 1)) as executor:
            query_results = list(executor.map(_fetch_sensor_events, sensors))
        for evs, err in query_results:
            if evs:
                events.extend(evs)
            if err:
                errors.append(err)
    except ResourceNotFoundError:
        return {
            "events": [],
            "sensor_count": len(sensors),
            "failed_sensor_count": 0,
            "errors": [],
            "fetched_at": end_time.isoformat(),
            "threshold": SPIKE_THRESHOLD,
            "history_ready": False,
            "message": "No stored PurpleAir history yet. Enable the history sync in the Azure Function App and wait for its first run.",
        }

    events.sort(key=lambda event: event["start"], reverse=True)
    result = {
        "events": events,
        "sensor_count": len(sensors),
        "failed_sensor_count": len(errors),
        "errors": errors[:10],
        "window_start": start_time.isoformat(),
        "window_end": end_time.isoformat(),
        "fetched_at": end_time.isoformat(),
        "threshold": SPIKE_THRESHOLD,
        "history_ready": True,
        "message": "",
    }
    _events_cache["data"] = result
    _events_cache["expires_at"] = time.time() + EVENTS_CACHE_SECONDS
    return result


def load_dashboard_data(force_refresh=False):
    """Read the registry + readings from Azure Table Storage and shape them for the page."""
    now_ts = time.time()
    if not force_refresh and _dashboard_cache["data"] and _dashboard_cache["expires_at"] > now_ts:
        return _dashboard_cache["data"]

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

    # 2. Recent readings query:
    # Instead of scanning every entity across weeks of history (tens of thousands of rows),
    # query recent window (last 48 hours) to make queries fast and cheap.
    table = service.get_table_client("PurpleAirData")
    since_48h = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    try:
        readings = list(table.query_entities(
            query_filter="timestamp ge @since",
            parameters={"since": since_48h}
        ))
        # If fewer than 10 readings in the last 48 hours (e.g. offline/testing), fall back gracefully
        if len(readings) < 10:
            readings = list(table.list_entities(results_per_page=100))[:100]
    except ResourceNotFoundError:
        readings = []
    readings.sort(key=lambda r: r.get("timestamp") or "", reverse=True)

    latest_by_sensor = {}
    for reading in readings:
        latest_by_sensor.setdefault(str(reading["PartitionKey"]), reading)

    # 3. One entry per sensor: registry metadata + its most recent reading
    sensors = []
    for sensor_id in sorted(set(registry) | set(latest_by_sensor)):
        reg = registry.get(sensor_id, {})
        latest = latest_by_sensor.get(sensor_id, {})
        pm25 = _first_float(latest.get("pm2_5"))

        sensor = {
            "id": sensor_id,
            "name": reg.get("Name") or latest.get("name") or f"Sensor {sensor_id}",
            "location": reg.get("Location") or "",
            "is_active": bool(reg.get("IsActive", False)),
            # Registry coordinates win; fall back to coordinates stored on the latest reading
            "latitude": _first_float(reg.get("Latitude"), latest.get("latitude")),
            "longitude": _first_float(reg.get("Longitude"), latest.get("longitude")),
            "pm2_5": pm25,
            "temperature": _first_float(latest.get("temperature")),
            "humidity": _first_float(latest.get("humidity")),
            "timestamp": latest.get("timestamp"),
        }
        sensor.update(pm25_to_aqi(pm25))
        sensors.append(sensor)

    # 4. Recent readings table (latest 50)
    names = {s["id"]: s["name"] for s in sensors}
    recent = []
    for reading in readings[:50]:
        sensor_id = str(reading["PartitionKey"])
        pm25 = _first_float(reading.get("pm2_5"))
        row = {
            "sensor_id": sensor_id,
            "name": names.get(sensor_id) or reading.get("name") or f"Sensor {sensor_id}",
            "pm2_5": pm25,
            "temperature": _first_float(reading.get("temperature")),
            "humidity": _first_float(reading.get("humidity")),
            "timestamp": reading.get("timestamp"),
        }
        row.update(pm25_to_aqi(pm25))
        recent.append(row)

    # 5. Summary cards
    with_data = [s for s in sensors if s["pm2_5"] is not None]
    avg_pm25 = sum(s["pm2_5"] for s in with_data) / len(with_data) if with_data else None
    summary = {
        "sensor_count": len(sensors),
        "active_count": sum(1 for s in sensors if s["is_active"]),
        "mapped_count": sum(1 for s in sensors if s["latitude"] is not None and s["longitude"] is not None),
        "reading_count": len(readings),
        "avg_pm2_5": avg_pm25,
        "avg_aqi": pm25_to_aqi(avg_pm25),
        "last_updated": readings[0].get("timestamp") if readings else None,
    }

    result = {"sensors": sensors, "recent": recent, "summary": summary}
    _dashboard_cache["data"] = result
    _dashboard_cache["expires_at"] = time.time() + DASHBOARD_CACHE_SECONDS
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


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting dashboard at http://localhost:{port}...")
    app.run(host='0.0.0.0', port=port, debug=True)
