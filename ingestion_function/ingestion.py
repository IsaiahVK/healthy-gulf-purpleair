"""
Shared PurpleAir -> Azure Table Storage ingestion logic.

Used by:
  - function_app.py        (the Azure Function timer trigger, every 5 minutes)
  - test_local_ingest.py   (runs the exact same logic from your machine)

All active sensors in the SensorRegistry table are fetched with ONE PurpleAir
request (GET /v1/sensors?show_only=...), requesting only the fields we store.
"""
import logging
from datetime import datetime, timezone

import requests
from azure.core.exceptions import ResourceExistsError
from azure.data.tables import TableServiceClient

PURPLEAIR_SENSORS_URL = "https://api.purpleair.com/v1/sensors"
# PurpleAir charges API points per field returned, so only request what we store
FIELDS = ["name", "latitude", "longitude", "pm2.5", "temperature", "humidity", "last_seen"]
# Safety valve for very large registries; normally everything fits in one request
MAX_SENSORS_PER_REQUEST = 100

REGISTRY_TABLE = "SensorRegistry"
DATA_TABLE = "PurpleAirData"

# PurpleAir field name -> Table Storage property name (property names can't contain dots)
NUMERIC_FIELDS = {
    "pm2.5": "pm2_5",
    "temperature": "temperature",
    "humidity": "humidity",
    "latitude": "latitude",
    "longitude": "longitude",
}


def _get_or_create_table(service, name):
    client = service.get_table_client(table_name=name)
    try:
        client.create_table()
    except ResourceExistsError:
        pass
    return client


def fetch_sensor_readings(api_key, sensor_ids):
    """Fetch current readings for many sensors at once. Returns {sensor_id: {field: value}}.

    Note: by default PurpleAir omits sensors that haven't reported in the last 7 days,
    so offline sensors simply won't appear in the result (we don't want stale values).
    """
    results = {}
    for i in range(0, len(sensor_ids), MAX_SENSORS_PER_REQUEST):
        batch = sensor_ids[i:i + MAX_SENSORS_PER_REQUEST]
        response = requests.get(
            PURPLEAIR_SENSORS_URL,
            headers={"X-API-Key": api_key},
            params={"fields": ",".join(FIELDS), "show_only": ",".join(batch)},
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"PurpleAir request failed: HTTP {response.status_code} - {response.text[:300]}")

        payload = response.json()
        columns = payload.get("fields", [])
        for row in payload.get("data", []):
            record = dict(zip(columns, row))
            results[str(record.get("sensor_index"))] = record
    return results


def run_ingestion(api_key, connection_string, logger=logging):
    """Fetch all active sensors from PurpleAir and store one reading per sensor.

    Returns a summary dict: {"requested": int, "saved": int, "missing": [sensor_id, ...]}
    """
    service = TableServiceClient.from_connection_string(conn_str=connection_string)

    # 1. Which sensors should we ingest?
    registry = _get_or_create_table(service, REGISTRY_TABLE)
    active_sensors = list(registry.query_entities("IsActive eq true"))
    if not active_sensors:
        logger.warning("No active sensors found in SensorRegistry. Nothing to ingest.")
        return {"requested": 0, "saved": 0, "missing": []}

    registry_names = {str(s["RowKey"]): s.get("Name") for s in active_sensors}
    sensor_ids = list(registry_names)
    logger.info(f"Requesting {len(sensor_ids)} active sensor(s) from PurpleAir in a single batch...")

    # 2. One PurpleAir request for all of them
    readings = fetch_sensor_readings(api_key, sensor_ids)

    # 3. Store one row per sensor
    data_client = _get_or_create_table(service, DATA_TABLE)
    utc_now = datetime.now(timezone.utc)
    row_prefix = utc_now.strftime("%Y%m%d%H%M%S")

    saved, missing = 0, []
    for sensor_id in sensor_ids:
        record = readings.get(sensor_id)
        if record is None:
            missing.append(sensor_id)
            continue

        entity = {
            "PartitionKey": sensor_id,
            "RowKey": f"{row_prefix}_{sensor_id}",
            "name": record.get("name") or registry_names.get(sensor_id) or f"Sensor {sensor_id}",
            "timestamp": utc_now.isoformat(),
        }
        # Only store values PurpleAir actually returned (null stays absent rather than becoming 0)
        for source_field, property_name in NUMERIC_FIELDS.items():
            value = record.get(source_field)
            if value is not None:
                entity[property_name] = float(value)
        # When the sensor itself last reported (PurpleAir returns a Unix timestamp)
        if record.get("last_seen"):
            entity["last_seen"] = datetime.fromtimestamp(record["last_seen"], tz=timezone.utc).isoformat()

        data_client.upsert_entity(entity=entity)
        saved += 1
        logger.info(f"Saved #{sensor_id} ({entity['name']}): PM2.5={entity.get('pm2_5')}")

    if missing:
        logger.warning(
            f"{len(missing)} sensor(s) not returned by PurpleAir "
            f"(offline > 7 days, private, or invalid ID): {', '.join(missing)}"
        )
    logger.info(f"Ingestion complete: {saved}/{len(sensor_ids)} sensor(s) saved.")
    return {"requested": len(sensor_ids), "saved": saved, "missing": missing}

