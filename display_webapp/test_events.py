import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from display_webapp.app import _decode_history, app, detect_spike_events


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
        self.assertIn(b"refresh-events", response.data)

    def test_events_endpoint_reports_missing_api_key(self):
        with patch.dict("os.environ", {"PURPLEAIR_API_KEY": ""}):
            response = app.test_client().get("/api/events")

        self.assertEqual(response.status_code, 503)
        self.assertIn(b"PurpleAir READ key", response.data)


if __name__ == "__main__":
    unittest.main()