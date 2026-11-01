"""
Rebuilds the SpikeEvents table from readings already stored in PurpleAirData.

    python backfill_spike_events.py            # last 7 days
    python backfill_spike_events.py --days 30
    python backfill_spike_events.py --dry-run  # print events without writing

New readings update SpikeEvents automatically; run this once after deploying,
or again after changing the spike threshold. It makes no PurpleAir API calls.
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableServiceClient
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=ROOT / ".env")

sys.path.insert(0, str(ROOT / "ingestion_function"))
from ingestion import (  # noqa: E402
    DATA_TABLE,
    REGISTRY_TABLE,
    SPIKE_STATE_PARTITION,
    _parse_timestamp,
    get_spike_tables,
    record_spike_reading,
)


class MemoryTable:
    """Just enough of TableClient for record_spike_reading, kept in memory."""

    def __init__(self):
        self.rows = {}

    def get_entity(self, partition_key, row_key):
        try:
            return dict(self.rows[(partition_key, row_key)])
        except KeyError:
            raise ResourceNotFoundError("not found")

    def upsert_entity(self, entity):
        self.rows[(entity["PartitionKey"], entity["RowKey"])] = dict(entity)

    def delete_entity(self, partition_key, row_key):
        self.rows.pop((partition_key, row_key), None)


def load_readings(data_table, sensor_id, since):
    rows = data_table.query_entities(
        query_filter="PartitionKey eq @sensor_id and RowKey ge @since",
        parameters={"sensor_id": sensor_id, "since": since.strftime("%Y%m%d%H%M%S")},
        select=["timestamp", "pm2_5"],
    )
    readings = [(_parse_timestamp(row["timestamp"]), row.get("pm2_5")) for row in rows if row.get("timestamp")]
    return sorted(readings, key=lambda reading: reading[0])


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=7, help="How many days of stored readings to replay (default 7)")
    parser.add_argument("--dry-run", action="store_true", help="Print the events without writing them")
    args = parser.parse_args()

    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        print("ERROR: Missing AZURE_STORAGE_CONNECTION_STRING in .env")
        sys.exit(1)

    service = TableServiceClient.from_connection_string(conn_str=connection_string)
    data_table = service.get_table_client(DATA_TABLE)
    sensors = list(service.get_table_client(REGISTRY_TABLE).query_entities("IsActive eq true"))
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    if not args.dry_run:
        events_table, state_table = get_spike_tables(service)

    total = 0
    for sensor in sensors:
        sensor_id = str(sensor["RowKey"])
        sensor_name = sensor.get("Name") or f"Sensor {sensor_id}"
        events, state = MemoryTable(), MemoryTable()
        readings = load_readings(data_table, sensor_id, since)
        for timestamp, pm2_5 in readings:
            record_spike_reading(events, state, sensor_id, sensor_name, timestamp, pm2_5)

        print(f"#{sensor_id} ({sensor_name}): {len(readings)} reading(s) -> {len(events.rows)} event(s)")
        for event in sorted(events.rows.values(), key=lambda e: e["RowKey"]):
            print(f"    {event['start_time']} to {event['end_time']}  peak {event['peak_pm2_5']:.1f}")
        total += len(events.rows)
        if args.dry_run:
            continue

        operations = [("upsert", event) for event in events.rows.values()]
        for offset in range(0, len(operations), 100):
            events_table.submit_transaction(operations[offset:offset + 100])
        open_event = state.rows.get((SPIKE_STATE_PARTITION, sensor_id))
        if open_event:
            state_table.upsert_entity(entity=open_event)
        else:
            state_table.delete_entity(partition_key=SPIKE_STATE_PARTITION, row_key=sensor_id)

    print(f"\n{'Found' if args.dry_run else 'Wrote'} {total} event(s) for {len(sensors)} sensor(s).")


if __name__ == "__main__":
    main()
