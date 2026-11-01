import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from azure.core.exceptions import ResourceNotFoundError

from display_webapp.app import app


class SpikeEventTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=3)

    def service_with(self, events_table, registry_rows):
        registry = MagicMock()
        registry.list_entities.return_value = registry_rows
        service = MagicMock()
        service.get_table_client.side_effect = lambda table: {
            "SensorRegistry": registry,
            "SpikeEvents": events_table,
        }[table]
        return service

    def get_events(self, service):
        with patch.dict("os.environ", {"AZURE_STORAGE_CONNECTION_STRING": "storage", "PURPLEAIR_API_KEY": ""}),                 patch("display_webapp.app.TableServiceClient.from_connection_string", return_value=service):
            return app.test_client().get("/api/events")

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
        self.assertIn(b"updated as each new reading arrives", response.data)
        self.assertIn(b"event-sensor-filter", response.data)
        self.assertNotIn(b"refresh-events", response.data)

    def test_events_endpoint_reads_spike_events_table(self):
        events_table = MagicMock()
        events_table.query_entities.return_value = [{
            "PartitionKey": "123",
            "RowKey": self.start.strftime("%Y%m%d%H%M%S"),
            "sensor_name": "Old name",
            "start_time": self.start.isoformat(),
            "end_time": (self.start + timedelta(minutes=10)).isoformat(),
            "peak_pm2_5": 40.0,
            "total_pm2_5": 114.0,
            "reading_count": 3,
        }]
        service = self.service_with(events_table, [
            {"RowKey": "123", "Name": "Test sensor", "Location": "Test location", "IsActive": True},
        ])

        response = self.get_events(service)

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["history_ready"])
        self.assertEqual(payload["sensor_count"], 1)
        [event] = payload["events"]
        self.assertEqual(event["sensor_name"], "Test sensor")
        self.assertEqual(event["location"], "Test location")
        self.assertEqual(event["peak_pm2_5"], 40.0)
        self.assertEqual(event["mean_pm2_5"], 38.0)
        self.assertEqual(event["duration_minutes"], 15)
        events_table.query_entities.assert_called_once()

    def test_events_endpoint_is_empty_before_any_events_are_recorded(self):
        events_table = MagicMock()
        events_table.query_entities.side_effect = ResourceNotFoundError("table missing")
        service = self.service_with(events_table, [{"RowKey": "123", "Name": "Test sensor", "IsActive": True}])

        response = self.get_events(service)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["events"], [])
        self.assertEqual(response.get_json()["failed_sensor_count"], 0)


if __name__ == "__main__":
    unittest.main()
