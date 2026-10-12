"""
One-time helper: adds Latitude/Longitude to every sensor in the SensorRegistry table.

Sensor locations are static metadata, so they live in the registry (not in every reading).
Run this whenever you add new sensors to the registry:

    python backfill_sensor_locations.py

API cost: sensors missing coordinates are looked up in batches with a single
/v1/sensors request (show_only=...), requesting only name/latitude/longitude.
"""
import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from azure.data.tables import TableServiceClient, UpdateMode

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

api_key = os.environ.get("PURPLEAIR_API_KEY")
connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

if not api_key or not connection_string:
    raise SystemExit("Missing PURPLEAIR_API_KEY or AZURE_STORAGE_CONNECTION_STRING in .env")

service = TableServiceClient.from_connection_string(conn_str=connection_string)
registry = service.get_table_client(table_name="SensorRegistry")

sensors = list(registry.list_entities())
print(f"Found {len(sensors)} sensor(s) in SensorRegistry.")

missing = {
    str(s["RowKey"]): s for s in sensors
    if s.get("Latitude") is None or s.get("Longitude") is None
}
skipped = len(sensors) - len(missing)
print(f"{skipped} already have coordinates; looking up {len(missing)}.\n")

# Fetch coordinates for all missing sensors in batches (one API request per batch)
results = {}
ids = list(missing)
BATCH_SIZE = 100
for i in range(0, len(ids), BATCH_SIZE):
    batch = ids[i:i + BATCH_SIZE]
    response = requests.get(
        "https://api.purpleair.com/v1/sensors",
        headers={"X-API-Key": api_key},
        # max_age=0 includes sensors that have been offline for a while (locations don't go stale)
        params={"fields": "name,latitude,longitude", "show_only": ",".join(batch), "max_age": 0},
        timeout=30,
    )
    if response.status_code != 200:
        raise SystemExit(f"PurpleAir request failed: HTTP {response.status_code} - {response.text[:300]}")

    payload = response.json()
    columns = payload.get("fields", [])
    for row in payload.get("data", []):
        record = dict(zip(columns, row))
        results[str(record.get("sensor_index"))] = record

updated = failed = 0
for sensor_id, sensor in missing.items():
    record = results.get(sensor_id)
    lat = record.get("latitude") if record else None
    lon = record.get("longitude") if record else None
    if lat is None or lon is None:
        print(f"  x #{sensor_id} {sensor.get('Name', '')}: not returned by PurpleAir "
              f"(check the ID, or the sensor may be private/offline)")
        failed += 1
        continue

    patch = {
        "PartitionKey": sensor["PartitionKey"],
        "RowKey": sensor_id,
        "Latitude": float(lat),
        "Longitude": float(lon),
    }
    # Fill in a name only if the registry row doesn't already have one
    if not sensor.get("Name") and record.get("name"):
        patch["Name"] = record["name"]

    # MERGE keeps existing properties (Name, IsActive, Location, ...) intact
    registry.update_entity(entity=patch, mode=UpdateMode.MERGE)
    print(f"  + #{sensor_id} {sensor.get('Name', '')}: ({lat}, {lon})")
    updated += 1

print(f"\nDone. Updated: {updated}, already had coordinates: {skipped}, failed: {failed}")
