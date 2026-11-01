import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from azure.core.exceptions import ResourceNotFoundError

from ingestion_function.ingestion import SPIKE_STATE_PARTITION, process_sensor_push, record_spike_reading


class FakeTable:
    def __init__(self):
        self.rows = {}

    def get_entity(self, partition_key, row_key):
        try:
            return dict(self.rows[(partition_key, row_key)])
        except KeyError:
            raise ResourceNotFoundError("not found")

    def upsert_entity(self, entity, mode=None):
        self.rows[(entity["PartitionKey"], entity["RowKey"])] = dict(entity)

    def delete_entity(self, partition_key, row_key):
        self.rows.pop((partition_key, row_key), None)

    def create_table(self):
        pass


class RecordSpikeReadingTests(unittest.TestCase):
    def setUp(self):
        self.events = FakeTable()
        self.state = FakeTable()
        self.start = datetime(2026, 10, 1, tzinfo=timezone.utc)

    def record(self, minutes, pm2_5, sensor_id="123"):
        record_spike_reading(self.events, self.state, sensor_id, "Test sensor",
                             self.start + timedelta(minutes=minutes), pm2_5)

    def event_list(self):
        return sorted(self.events.rows.values(), key=lambda event: event["RowKey"])

    def test_threshold_is_strictly_above_35(self):
        self.record(0, 35.0)
        self.assertEqual(self.events.rows, {})

        self.record(5, 35.1)
        self.assertEqual(len(self.events.rows), 1)
        self.assertEqual(self.event_list()[0]["peak_pm2_5"], 35.1)

    def test_consecutive_readings_extend_one_event(self):
        for minutes, value in [(0, 36.0), (5, 42.0), (10, 38.0)]:
            self.record(minutes, value)

        [event] = self.event_list()
        self.assertEqual(event["reading_count"], 3)
        self.assertEqual(event["peak_pm2_5"], 42.0)
        self.assertAlmostEqual(event["total_pm2_5"], 116.0)
        self.assertEqual(event["start_time"], self.start.isoformat())
        self.assertEqual(event["end_time"], (self.start + timedelta(minutes=10)).isoformat())
        self.assertIn((SPIKE_STATE_PARTITION, "123"), self.state.rows)

    def test_reading_at_or_below_threshold_closes_event(self):
        self.record(0, 40.0)
        self.record(5, 12.0)
        self.record(10, 41.0)

        self.assertEqual(len(self.events.rows), 2)

    def test_closing_reading_clears_state(self):
        self.record(0, 40.0)
        self.record(5, 12.0)

        self.assertEqual(self.state.rows, {})

    def test_gap_over_20_minutes_starts_new_event(self):
        self.record(0, 40.0)
        self.record(25, 41.0)

        self.assertEqual([event["reading_count"] for event in self.event_list()], [1, 1])

    def test_missing_values_are_ignored(self):
        self.record(0, 40.0)
        self.record(5, None)
        self.record(10, 41.0)

        [event] = self.event_list()
        self.assertEqual(event["reading_count"], 2)

    def test_duplicate_or_older_readings_are_ignored(self):
        self.record(10, 40.0)
        self.record(10, 90.0)
        self.record(5, 90.0)

        [event] = self.event_list()
        self.assertEqual(event["reading_count"], 1)
        self.assertEqual(event["peak_pm2_5"], 40.0)

    def test_sensors_are_tracked_independently(self):
        self.record(0, 40.0, sensor_id="1")
        self.record(5, 10.0, sensor_id="2")
        self.record(5, 41.0, sensor_id="1")

        [event] = self.event_list()
        self.assertEqual(event["PartitionKey"], "1")
        self.assertEqual(event["reading_count"], 2)


class PushUpdatesSpikeEventsTests(unittest.TestCase):
    def test_direct_push_above_threshold_records_event(self):
        tables = {}
        service = MagicMock()
        service.get_table_client.side_effect = lambda table_name: tables.setdefault(table_name, FakeTable())

        with patch("ingestion_function.ingestion.TableServiceClient.from_connection_string", return_value=service):
            process_sensor_push(
                {"SensorId": "abc", "DateTime": "2026/10/19T20:45:00z", "pm2_5_atm": 50.0, "pm2_5_atm_b": 52.0},
                "storage",
            )

        [event] = tables["SpikeEvents"].rows.values()
        self.assertEqual(event["PartitionKey"], "abc")
        self.assertEqual(event["peak_pm2_5"], 51.0)
        self.assertEqual(event["start_time"], "2026-10-19T20:45:00+00:00")


if __name__ == "__main__":
    unittest.main()
