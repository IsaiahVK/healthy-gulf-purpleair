"""
Syncs the SensorRegistry table in Azure with sensors.csv (the source of truth in git).

    python sync_sensor_registry.py                          # add/update sensors from the CSV
    python sync_sensor_registry.py --prune                  # ...and remove registry sensors NOT in the CSV
    python sync_sensor_registry.py --prune --delete-readings # ...and also delete stored readings for removed sensors

CSV columns: sensor_id, name, location (optional), is_active (optional, default true)

Notes:
- Existing properties not in the CSV (e.g. Latitude/Longitude from the backfill) are preserved.
- Readings are backed up to backups/ as JSON before they are deleted.
- After adding new sensors, run `python backfill_sensor_locations.py` so they appear on the map.
"""
import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableServiceClient, UpdateMode

ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=ROOT / ".env")

PARTITION_KEY = "HealthyGulf"

parser = argparse.ArgumentParser(description="Sync SensorRegistry with a CSV file.")
parser.add_argument("--csv", default=str(ROOT / "sensors.csv"), help="Path to the sensor CSV")
parser.add_argument("--prune", action="store_true", help="Remove registry sensors that are not in the CSV")
parser.add_argument("--delete-readings", action="store_true",
                    help="With --prune: also delete PurpleAirData readings for removed sensors (backed up first)")
args = parser.parse_args()

if args.delete_readings and not args.prune:
    raise SystemExit("--delete-readings only makes sense together with --prune")

connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    raise SystemExit("Missing AZURE_STORAGE_CONNECTION_STRING in .env")

# ---- Read the CSV ----
desired = {}
with open(args.csv, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        sensor_id = (row.get("sensor_id") or "").strip()
        if not sensor_id:
            continue
        if not sensor_id.isdigit():
            raise SystemExit(f"Invalid sensor_id '{sensor_id}' in {args.csv}")
        is_active = (row.get("is_active") or "true").strip().lower() not in ("false", "0", "no")
        desired[sensor_id] = {
            "name": (row.get("name") or "").strip(),
            "location": (row.get("location") or "").strip(),
            "is_active": is_active,
        }
print(f"CSV contains {len(desired)} sensor(s).")

service = TableServiceClient.from_connection_string(conn_str=connection_string)
registry = service.get_table_client(table_name="SensorRegistry")
try:
    registry.create_table()
except Exception:
    pass

existing = {str(e["RowKey"]): e for e in registry.list_entities()}

# ---- Add / update ----
print("\nAdding / updating sensors:")
for sensor_id, info in desired.items():
    entity = {
        "PartitionKey": existing.get(sensor_id, {}).get("PartitionKey", PARTITION_KEY),
        "RowKey": sensor_id,
        "Name": info["name"],
        "IsActive": info["is_active"],
    }
    if info["location"]:
        entity["Location"] = info["location"]
    # MERGE keeps properties we don't send (e.g. Latitude/Longitude)
    registry.upsert_entity(entity=entity, mode=UpdateMode.MERGE)
    action = "updated" if sensor_id in existing else "added"
    print(f"  {'~' if action == 'updated' else '+'} #{sensor_id} {info['name']} ({action})")

# ---- Prune ----
if args.prune:
    to_remove = [e for sid, e in existing.items() if sid not in desired]
    print(f"\nRemoving {len(to_remove)} sensor(s) not in the CSV:")
    for entity in to_remove:
        registry.delete_entity(partition_key=entity["PartitionKey"], row_key=entity["RowKey"])
        print(f"  - #{entity['RowKey']} {entity.get('Name', '')}")

    if args.delete_readings and to_remove:
        data = service.get_table_client(table_name="PurpleAirData")
        removed_ids = [str(e["RowKey"]) for e in to_remove]
        readings = []
        try:
            for sensor_id in removed_ids:
                readings.extend(data.query_entities("PartitionKey eq @pk", parameters={"pk": sensor_id}))
        except ResourceNotFoundError:
            readings = []

        if readings:
            backup_dir = ROOT / "backups"
            backup_dir.mkdir(exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            backup_file = backup_dir / f"deleted_readings_{stamp}.json"
            with open(backup_file, "w", encoding="utf-8") as f:
                json.dump([dict(r) for r in readings], f, indent=2, default=str)
            print(f"\nBacked up {len(readings)} reading(s) to {backup_file.relative_to(ROOT)}")

            for r in readings:
                data.delete_entity(partition_key=r["PartitionKey"], row_key=r["RowKey"])
            print(f"Deleted {len(readings)} reading(s) for removed sensors.")
        else:
            print("\nNo stored readings found for removed sensors.")

print("\nDone. Run `python backfill_sensor_locations.py` to add coordinates for any new sensors.")

