"""
Shared PurpleAir -> Azure Table Storage ingestion logic.

Supports TWO ingestion methods:
  1. SCHEDULED PULL:
     - All active sensors with IngestionType != "push" are fetched in ONE
       batched GET request from PurpleAir API (GET /v1/sensors?show_only=...).
  2. DIRECT PUSH (Data Processor / Webhook):
     - An owned PurpleAir sensor pushes telemetry directly to an HTTP endpoint
       (POST /api/sensor_push). The payload is parsed and saved to PurpleAirData.
"""
import logging
import time
from datetime import datetime, timedelta, timezone

import requests
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode

PURPLEAIR_SENSORS_URL = "https://api.purpleair.com/v1/sensors"
# PurpleAir charges API points per field returned, so only request what we store
FIELDS = ["name", "latitude", "longitude", "pm2.5", "temperature", "humidity", "last_seen"]
# Safety valve for very large registries; normally everything fits in one request
MAX_SENSORS_PER_REQUEST = 100

REGISTRY_TABLE = "SensorRegistry"
DATA_TABLE = "PurpleAirData"
HISTORY_TABLE = "PurpleAirHistory"
HISTORY_STATE_TABLE = "PurpleAirHistorySyncState"
HISTORY_FIELDS = ["pm2.5_atm"]
HISTORY_INITIAL_DAYS = 7
HISTORY_OVERLAP_MINUTES = 20

# PurpleAir field name -> Table Storage property name (property names can't contain dots)
NUMERIC_FIELDS = {
    "pm2.5": "pm2_5",
    "temperature": "temperature",
    "humidity": "humidity",
    "latitude": "latitude",
    "longitude": "longitude",
}


def _first_float(*values):
    for v in values:
        if v is None or v == "":
            continue
        try:
            return float(v)
        except (ValueError, TypeError):
            continue
    return None


def _get_or_create_table(service, name):
    client = service.get_table_client(table_name=name)
    try:
        client.create_table()
    except ResourceExistsError:
        pass
    return client


def _parse_history_timestamp(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    timestamp = str(value)
    try:
        return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    except ValueError:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _decode_history(payload):
    fields = payload.get("fields", [])
    readings = []
    for row in payload.get("data", []):
        if isinstance(row, dict):
            values = row
        elif isinstance(row, str):
            values = dict(zip(fields, row.split(",")))
        else:
            values = dict(zip(fields, row))
        timestamp = values.get("time_stamp") or values.get("timestamp")
        pm2_5 = values.get("pm2.5_atm")
        if timestamp is None or pm2_5 is None:
            continue
        try:
            readings.append({"timestamp": _parse_history_timestamp(timestamp), "pm2_5": float(pm2_5)})
        except (TypeError, ValueError, OverflowError):
            continue
    return readings


def run_history_sync(api_key, connection_string, logger=logging, now=None):
    """Backfill 7 days once, then incrementally sync each sensor's 10-minute history."""
    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    registry = service.get_table_client(REGISTRY_TABLE)
    history = _get_or_create_table(service, HISTORY_TABLE)
    state = _get_or_create_table(service, HISTORY_STATE_TABLE)
    sync_time = now or datetime.now(timezone.utc)
    if sync_time.tzinfo is None:
        sync_time = sync_time.replace(tzinfo=timezone.utc)
    sync_time = sync_time.astimezone(timezone.utc)

    sensors = list(registry.query_entities("IsActive eq true"))
    if not sensors:
        logger.warning("No active sensors found in SensorRegistry. History sync skipped.")
        return {"requested": 0, "saved": 0, "failed": []}

    saved = 0
    failed = []
    next_request_at = time.monotonic()
    for sensor in sensors:
        sensor_id = str(sensor["RowKey"])
        try:
            try:
                checkpoint = state.get_entity(partition_key="PurpleAirHistory", row_key=sensor_id)
                previous_timestamp = _parse_history_timestamp(checkpoint["last_timestamp"])
                start_time = previous_timestamp - timedelta(minutes=HISTORY_OVERLAP_MINUTES)
            except ResourceNotFoundError:
                start_time = sync_time - timedelta(days=HISTORY_INITIAL_DAYS)

            delay = next_request_at - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            response = requests.get(
                f"{PURPLEAIR_SENSORS_URL}/{sensor_id}/history",
                headers={"X-API-Key": api_key},
                params={
                    "start_timestamp": int(start_time.timestamp()),
                    "end_timestamp": int(sync_time.timestamp()),
                    "average": 10,
                    "fields": ",".join(HISTORY_FIELDS),
                },
                timeout=30,
            )
            next_request_at = time.monotonic() + 1
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                raise RuntimeError(payload.get("description") or payload["error"])

            sensor_name = sensor.get("Name") or f"Sensor {sensor_id}"
            entities = []
            for reading in _decode_history(payload):
                timestamp = reading["timestamp"]
                entities.append({
                    "PartitionKey": sensor_id,
                    "RowKey": timestamp.strftime("%Y%m%d%H%M%S"),
                    "timestamp": timestamp.isoformat(),
                    "sensor_name": sensor_name,
                    "pm2_5": reading["pm2_5"],
                })

            for offset in range(0, len(entities), 100):
                batch = entities[offset:offset + 100]
                history.submit_transaction([("upsert", entity) for entity in batch])

            state.upsert_entity(entity={
                "PartitionKey": "PurpleAirHistory",
                "RowKey": sensor_id,
                "last_timestamp": sync_time.isoformat(),
                "last_sync": sync_time.isoformat(),
            })
            saved += len(entities)
            logger.info(f"History sync #{sensor_id} ({sensor_name}): saved {len(entities)} reading(s).")
        except Exception as exc:
            failed.append(sensor_id)
            logger.exception(f"History sync failed for sensor #{sensor_id}: {exc}")

    logger.info(f"History sync complete: saved {saved} reading(s); failed sensors: {len(failed)}.")
    return {"requested": len(sensors), "saved": saved, "failed": failed}

# ==============================================================================
# METHOD 1: Scheduled Pull (PurpleAir API)
# ==============================================================================

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
    """Fetch all active pull sensors from PurpleAir and store one reading per sensor.

    Returns a summary dict: {"requested": int, "saved": int, "missing": [sensor_id, ...]}
    """
    service = TableServiceClient.from_connection_string(conn_str=connection_string)

    # 1. Which sensors should we ingest?
    registry = _get_or_create_table(service, REGISTRY_TABLE)
    active_sensors = list(registry.query_entities("IsActive eq true"))
    if not active_sensors:
        logger.warning("No active sensors found in SensorRegistry. Nothing to ingest.")
        return {"requested": 0, "saved": 0, "missing": []}

    # Filter out sensors configured specifically for direct push (to save API points)
    pull_sensors = [s for s in active_sensors if (s.get("IngestionType") or "pull").lower() != "push"]
    if not pull_sensors:
        logger.info("All active sensors are configured for direct push. No API pull needed.")
        return {"requested": 0, "saved": 0, "missing": []}

    registry_names = {str(s["RowKey"]): s.get("Name") for s in pull_sensors}
    sensor_ids = list(registry_names)
    logger.info(f"Requesting {len(sensor_ids)} active pull sensor(s) from PurpleAir in a single batch...")

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
            "source": "api_pull",
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
    logger.info(f"API pull complete: {saved}/{len(sensor_ids)} sensor(s) saved.")
    return {"requested": len(sensor_ids), "saved": saved, "missing": missing}


# ==============================================================================
# METHOD 2: Direct Push (PurpleAir Data Processor Webhook)
# ==============================================================================

def process_sensor_push(payload, connection_string, logger=logging):
    """Processes an incoming HTTP POST telemetry packet sent directly by a PurpleAir sensor.

    Payload format varies slightly depending on firmware version and whether it's
    routed directly from device Wi-Fi or via PurpleAir cloud forwarder.
    Handles dual laser channels (Channel A/B average), temperature, humidity, and location.
    """
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON dictionary")

    logger.info(f"Processing incoming sensor push payload with fields: {list(payload.keys())}")

    # 1. Identify sensor ID / MAC address
    raw_id = (
        payload.get("sensor_index")
        or payload.get("SensorId")
        or payload.get("sensor_id")
        or payload.get("id")
        or payload.get("mac")
    )
    if not raw_id:
        raise ValueError("Payload missing sensor identifier (sensor_index, SensorId, etc.)")

    sensor_id = str(raw_id).strip()

    # 2. Extract environmental readings
    # PurpleAir dual laser sensors report pm2_5_atm and pm2_5_atm_b (or pm2.5)
    pm_a = _first_float(
        payload.get("pm2_5_atm"),
        payload.get("pm2.5_atm"),
        payload.get("pm2.5"),
        payload.get("pm2_5"),
        payload.get("pm2_5_cf_1"),
    )
    pm_b = _first_float(
        payload.get("pm2_5_atm_b"),
        payload.get("pm2.5_atm_b"),
        payload.get("pm2_5_b"),
    )
    if pm_a is not None and pm_b is not None:
        pm2_5 = round((pm_a + pm_b) / 2.0, 2)
    elif pm_a is not None:
        pm2_5 = pm_a
    elif pm_b is not None:
        pm2_5 = pm_b
    else:
        pm2_5 = None

    temp = _first_float(
        payload.get("current_temp_f"),
        payload.get("temperature"),
        payload.get("temp_f"),
        payload.get("temp"),
    )
    humidity = _first_float(
        payload.get("current_humidity"),
        payload.get("humidity"),
    )
    lat = _first_float(
        payload.get("latitude"),
        payload.get("lat"),
    )
    lon = _first_float(
        payload.get("longitude"),
        payload.get("lon"),
        payload.get("lng"),
    )

    # 3. Parse timestamp
    utc_now = datetime.now(timezone.utc)
    raw_dt = payload.get("DateTime") or payload.get("timestamp") or payload.get("time_stamp")
    if raw_dt:
        try:
            if isinstance(raw_dt, (int, float)):
                ts_iso = datetime.fromtimestamp(raw_dt, tz=timezone.utc).isoformat()
            else:
                cleaned = str(raw_dt).replace("/", "-").replace("z", "+00:00").replace("Z", "+00:00")
                parsed = datetime.fromisoformat(cleaned)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                ts_iso = parsed.isoformat()
        except Exception:
            ts_iso = utc_now.isoformat()
    else:
        ts_iso = utc_now.isoformat()

    service = TableServiceClient.from_connection_string(conn_str=connection_string)

    # 4. Check SensorRegistry for sensor name and metadata
    registry = _get_or_create_table(service, REGISTRY_TABLE)
    sensor_name = payload.get("name") or payload.get("sensor_name")

    existing_sensor = None
    try:
        existing_sensor = registry.get_entity(partition_key="HealthyGulf", row_key=sensor_id)
        if not sensor_name:
            sensor_name = existing_sensor.get("Name")
    except Exception:
        pass

    if not sensor_name:
        sensor_name = f"Sensor {sensor_id}"

    # If coordinates are present in the push payload, ensure registry has them
    if lat is not None and lon is not None:
        try:
            reg_patch = {
                "PartitionKey": "HealthyGulf",
                "RowKey": sensor_id,
                "Name": sensor_name,
                "Latitude": lat,
                "Longitude": lon,
                "IsActive": True,
                "IngestionType": "push",
            }
            registry.upsert_entity(entity=reg_patch, mode=UpdateMode.MERGE)
        except Exception as e:
            logger.warning(f"Could not update registry for push sensor {sensor_id}: {str(e)}")

    # 5. Insert reading into PurpleAirData
    data_client = _get_or_create_table(service, DATA_TABLE)
    row_prefix = utc_now.strftime("%Y%m%d%H%M%S")
    entity = {
        "PartitionKey": sensor_id,
        "RowKey": f"{row_prefix}_{sensor_id}",
        "name": sensor_name,
        "timestamp": ts_iso,
        "source": "direct_push",
    }
    if pm2_5 is not None:
        entity["pm2_5"] = float(pm2_5)
    if temp is not None:
        entity["temperature"] = float(temp)
    if humidity is not None:
        entity["humidity"] = float(humidity)
    if lat is not None:
        entity["latitude"] = float(lat)
    if lon is not None:
        entity["longitude"] = float(lon)

    data_client.upsert_entity(entity=entity)
    logger.info(f"Direct push saved: #{sensor_id} ({sensor_name}) -> PM2.5={pm2_5}, Temp={temp}F")

    return {
        "sensor_id": sensor_id,
        "name": sensor_name,
        "pm2_5": pm2_5,
        "temperature": temp,
        "humidity": humidity,
        "timestamp": ts_iso,
    }
