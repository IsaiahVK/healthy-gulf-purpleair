from flask import Flask, render_template, jsonify
import math
import os
from pathlib import Path
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


def load_dashboard_data():
    """Read the registry + readings from Azure Table Storage and shape them for the page."""
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

    # 2. All readings, newest first.
    # Fine for a PoC; with months of data, query per sensor partition instead of scanning everything.
    try:
        readings = list(service.get_table_client("PurpleAirData").list_entities())
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

    # 4. Recent readings table
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

    return {"sensors": sensors, "recent": recent, "summary": summary}


@app.route('/')
def index():
    try:
        data = load_dashboard_data()
    except Exception as e:
        return f"Error fetching data from Azure: {str(e)}", 500
    return render_template('index.html', **data, aqi_scale=AQI_BREAKPOINTS)


@app.route('/api/sensors')
def api_sensors():
    """JSON version of the per-sensor data (handy for debugging or future front-ends)."""
    try:
        data = load_dashboard_data()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"sensors": data["sensors"], "summary": data["summary"]})


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting dashboard at http://localhost:{port}...")
    app.run(host='0.0.0.0', port=port, debug=True)
