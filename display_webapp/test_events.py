import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from azure.core.exceptions import ResourceNotFoundError

from display_webapp.app import app, detect_spike_events
from ingestion_function.ingestion import _decode_history


class SpikeEventTests(unittest.TestCase):
    def setUp(self):
        self.sensor = {"id": "123", "name": "Test sensor", "location": "Test location"}
        self.start = datetime(2026, 10, 1, tzinfo=timezone.utc)

    def test_threshold_is_strictly_above_35(self):
        readings = [
            {"timestamp": self.start, "pm2_5": 35.0},
            {"timestamp": self.start + timedelta(minutes=10), "pm2_5": 35.1},
        ]

        events = detect_spike_events(self.sensor, readings)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["peak_pm2_5"], 35.1)

    def test_groups_adjacent_exceedances_and_reports_duration(self):
        readings = [
            {"timestamp": self.start + timedelta(minutes=10), "pm2_5": 38.0},
            {"timestamp": self.start, "pm2_5": 36.0},
            {"timestamp": self.start + timedelta(minutes=20), "pm2_5": 42.0},
        ]

        events = detect_spike_events(self.sensor, readings)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["duration_minutes"], 30)
        self.assertEqual(events[0]["reading_count"], 3)
        self.assertEqual(events[0]["peak_pm2_5"], 42.0)

    def test_splits_events_after_a_gap_over_20_minutes(self):
        readings = [
            {"timestamp": self.start, "pm2_5": 36.0},
            {"timestamp": self.start + timedelta(minutes=30), "pm2_5": 37.0},
        ]

        events = detect_spike_events(self.sensor, readings)

        self.assertEqual(len(events), 2)
        self.assertEqual([event["duration_minutes"] for event in events], [10, 10])

    def test_decodes_purpleair_history_columns_by_name(self):
        payload = {
            "fields": ["sensor_index", "pm2.5_atm", "time_stamp"],
            "data": [[123, 41.5, 1790812800]],
        }

        readings = _decode_history(payload)

        self.assertEqual(readings[0]["pm2_5"], 41.5)
        self.assertEqual(readings[0]["timestamp"].tzinfo, timezone.utc)

    def test_dashboard_renders_events_tab(self):
        dashboard_data = {
            "sensors": [],
            "recent": [],
            "summary": {
                "sensor_count": 0,
                "active_count": 0,
                "mapped_count": 0,
                "reading_count": 0,
                "avg_pm2_5": None,
                "avg_aqi": {"aqi": None, "category": "No data", "color": "#9e9e9e", "text_color": "#ffffff"},
                "last_updated": None,
            },
        }
        with patch("display_webapp.app.load_dashboard_data", return_value=dashboard_data):
            response = app.test_client().get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Recent PM2.5 spikes", response.data)
        self.assertIn(b"Stored PurpleAir 10-minute", response.data)
        self.assertIn(b"event-sensor-filter", response.data)
        self.assertNotIn(b"refresh-events", response.data)

    def test_events_endpoint_reads_storage_without_purpleair_key(self):
        registry = MagicMock()
        registry.list_entities.return_value = [{"RowKey": "123", "Name": "Test sensor", "Location": "Test location"}]
        history = MagicMock()
        history.query_entities.return_value = [
            {"timestamp": self.start.isoformat(), "pm2_5": 36.5},
            {"timestamp": (self.start + timedelta(minutes=10)).isoformat(), "pm2_5": 40.0},
        ]
        state = MagicMock()
        state.query_entities.return_value = [{"RowKey": "123"}]
        service = MagicMock()
        service.get_table_client.side_effect = lambda table: {
            "SensorRegistry": registry,
            "PurpleAirHistory": history,
            "PurpleAirHistorySyncState": state,
        }[table]

        with patch.dict("os.environ", {"AZURE_STORAGE_CONNECTION_STRING": "storage", "PURPLEAIR_API_KEY": ""}), \
                patch("display_webapp.app.TableServiceClient.from_connection_string", return_value=service):
            response = app.test_client().get("/api/events")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["history_ready"])
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["peak_pm2_5"], 40.0)
        history.query_entities.assert_called_once()

    def test_events_endpoint_explains_when_history_table_is_missing(self):
        registry = MagicMock()
        registry.list_entities.return_value = [{"RowKey": "123", "Name": "Test sensor"}]
        history = MagicMock()
        history.query_entities.side_effect = ResourceNotFoundError("table missing")
        state = MagicMock()
        state.query_entities.return_value = [{"RowKey": "123"}]
        service = MagicMock()
        service.get_table_client.side_effect = lambda table: {
            "SensorRegistry": registry,
            "PurpleAirHistory": history,
            "PurpleAirHistorySyncState": state,
        }[table]

        with patch.dict("os.environ", {"AZURE_STORAGE_CONNECTION_STRING": "storage"}), \
                patch("display_webapp.app.TableServiceClient.from_connection_string", return_value=service):
            response = app.test_client().get("/api/events")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["history_ready"])
        self.assertIn("Enable the history sync", response.get_json()["message"])


if __name__ == "__main__":
    unittest.main()