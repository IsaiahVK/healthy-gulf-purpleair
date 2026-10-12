"""
One-time helper: adds Latitude/Longitude to every sensor in the SensorRegistry table.

Sensor locations are static metadata, so they live in the registry (not in every reading).
Run this whenever you add new sensors to the registry:

    python backfill_sensor_locations.py

API cost: one small request per sensor that is missing coordinates. Only the
name/latitude/longitude fields are requested, which keeps PurpleAir point usage minimal.
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
print(f"Found {len(sensors)} sensor(s) in SensorRegistry.\n")

updated = skipped = failed = 0
for sensor in sensors:
    sensor_id = sensor["RowKey"]

    if sensor.get("Latitude") is not None and sensor.get("Longitude") is not None:
        print(f"  = #{sensor_id}: already has coordinates, skipping")
        skipped += 1
        continue

    response = requests.get(
        f"https://api.purpleair.com/v1/sensors/{sensor_id}",
        headers={"X-API-Key": api_key},
        params={"fields": "name,latitude,longitude"},
        timeout=15,
    )
    if response.status_code != 200:
        print(f"  x #{sensor_id}: HTTP {response.status_code} - {response.text[:200]}")
        failed += 1
        continue

    data = response.json().get("sensor", {})
    lat, lon = data.get("latitude"), data.get("longitude")
    if lat is None or lon is None:
        print(f"  x #{sensor_id}: PurpleAir returned no coordinates")
        failed += 1
        continue

    patch = {
        "PartitionKey": sensor["PartitionKey"],
        "RowKey": sensor_id,
        "Latitude": float(lat),
        "Longitude": float(lon),
    }
    # Fill in a name only if the registry row doesn't already have one
    if not sensor.get("Name") and data.get("name"):
        patch["Name"] = data["name"]

    # MERGE keeps existing properties (Name, IsActive, Location, ...) intact
    registry.update_entity(entity=patch, mode=UpdateMode.MERGE)
    print(f"  + #{sensor_id}: saved ({lat}, {lon})")
    updated += 1

print(f"\nDone. Updated: {updated}, skipped: {skipped}, failed: {failed}")

