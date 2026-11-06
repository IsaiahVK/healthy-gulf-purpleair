import json
import time
import unittest
from unittest.mock import MagicMock, patch

from azure.core.exceptions import ResourceExistsError, ResourceModifiedError, ResourceNotFoundError
from azure.data.tables import UpdateMode
import requests

from display_webapp.app import LIVE_MAX_MINUTES, LIVE_SESSION_PARTITION, LIVE_SESSION_ROW, app

ENV = {
    "AZURE_STORAGE_CONNECTION_STRING": "storage",
    "PURPLEAIR_API_KEY": "secret-api-key",
    "LIVE_SENSOR_INDEX": "12345",
    "LIVE_SENSOR_READ_KEY": "secret-read-key",
}


class Row(dict):
    metadata = {}


class FakeTable:
    def __init__(self):
        self.rows = {}
        self.version = 0
        self.exists = False

    def create_table(self):
        if self.exists:
            raise ResourceExistsError("exists")
        self.exists = True

    def get_entity(self, partition_key, row_key):
        if (partition_key, row_key) not in self.rows:
            raise ResourceNotFoundError("missing")
        row = Row(self.rows[(partition_key, row_key)])
        row.metadata = {"etag": self.version}
        return row

    def upsert_entity(self, entity, mode=None):
        self.version += 1
        self.rows[(entity["PartitionKey"], entity["RowKey"])] = dict(entity)

    def update_entity(self, entity, mode=UpdateMode.MERGE, etag=None, match_condition=None):
        if etag is not None and etag != self.version:
            raise ResourceModifiedError("changed")
        self.version += 1
        key = (entity["PartitionKey"], entity["RowKey"])
        self.rows[key] = {**self.rows[key], **entity} if mode == UpdateMode.MERGE else dict(entity)

    def query_entities(self, query_filter, parameters):
        return [
            row for (partition, key), row in self.rows.items()
            if partition == parameters["session_id"] and key > parameters["after"]
        ]


class LiveSensorTests(unittest.TestCase):
    def setUp(self):
        self.table = FakeTable()
        service = MagicMock()
        service.get_table_client.return_value = self.table
        self.request = MagicMock()
        self.report(int(time.time()) - 10)
        patches = [
            patch.dict("os.environ", ENV),
            patch("display_webapp.app.TableServiceClient.from_connection_string", return_value=service),
            patch("display_webapp.app.requests.get", self.request),  # tests never reach PurpleAir
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        self.client = app.test_client()

    def report(self, last_seen, pm_a=40.0, pm_b=42.0):
        self.request.return_value.json.return_value = {"sensor": {
            "last_seen": last_seen, "pm2.5_atm_a": pm_a, "pm2.5_atm_b": pm_b, "temperature": 80, "humidity": 50,
        }}

    def session(self):
        return self.table.rows[(LIVE_SESSION_PARTITION, LIVE_SESSION_ROW)]

    def test_nothing_is_requested_until_recording_starts(self):
        state = self.client.get("/api/live").get_json()

        self.assertFalse(state["recording"])
        self.request.assert_not_called()

    def test_start_records_first_reading_with_server_side_keys(self):
        state = self.client.post("/api/live/start").get_json()

        self.assertTrue(state["recording"])
        [sample] = state["samples"]
        self.assertEqual(sample["pm"], 41.0)
        self.assertEqual(sample["category"], "Unhealthy for Sensitive Groups")
        self.assertEqual(self.request.call_args.kwargs["headers"]["X-API-Key"], "secret-api-key")
        self.assertEqual(self.request.call_args.kwargs["params"]["read_key"], "secret-read-key")
        self.assertNotIn("secret", json.dumps(state))

    def test_next_request_waits_until_a_report_is_due(self):
        self.client.post("/api/live/start")
        for _ in range(5):
            self.client.get("/api/live")

        self.assertEqual(self.request.call_count, 1)

    def test_new_report_is_added_when_due(self):
        self.client.post("/api/live/start")
        self.report(int(time.time()), pm_a=10.0, pm_b=12.0)
        self.session()["next_fetch_at"] = 0
        state = self.client.get("/api/live").get_json()

        self.assertEqual([sample["pm"] for sample in state["samples"]], [41.0, 11.0])

    def test_only_one_worker_calls_purpleair(self):
        self.client.post("/api/live/start")
        self.request.reset_mock()
        self.session()["next_fetch_at"] = 0
        session = self.table.get_entity(LIVE_SESSION_PARTITION, LIVE_SESSION_ROW)
        self.table.version += 1  # another worker claimed it after this one read the session
        with patch.object(self.table, "get_entity", return_value=session):
            self.client.get("/api/live")

        self.request.assert_not_called()

    def test_repeated_report_is_not_stored_twice(self):
        self.client.post("/api/live/start")
        self.session()["next_fetch_at"] = 0
        state = self.client.get("/api/live").get_json()

        self.assertEqual(self.request.call_count, 2)
        self.assertEqual(len(state["samples"]), 1)

    def test_recording_stops_by_itself_after_the_limit(self):
        self.client.post("/api/live/start")
        self.assertLessEqual(self.session()["ends_at"] - time.time(), LIVE_MAX_MINUTES * 60)
        self.request.reset_mock()
        self.session()["ends_at"] = int(time.time()) - 1
        self.session()["next_fetch_at"] = 0
        state = self.client.get("/api/live").get_json()

        self.assertFalse(state["recording"])
        self.request.assert_not_called()

    def test_stop_ends_requests_and_keeps_readings(self):
        self.client.post("/api/live/start")
        self.session()["next_fetch_at"] = 0
        self.request.reset_mock()
        state = self.client.post("/api/live/stop").get_json()
        self.client.get("/api/live")

        self.assertFalse(state["recording"])
        self.assertEqual(len(state["samples"]), 1)
        self.request.assert_not_called()

    def test_start_while_recording_does_not_restart(self):
        self.client.post("/api/live/start")
        self.session()["session_id"] = "keep-me"
        self.client.post("/api/live/start")

        self.assertEqual(self.session()["session_id"], "keep-me")

    def test_api_error_is_reported_without_leaking_keys(self):
        error = requests.HTTPError("https://api.purpleair.com/?read_key=secret-read-key")
        error.response = MagicMock(status_code=402)
        self.request.side_effect = error
        state = self.client.post("/api/live/start").get_json()

        self.assertIn("402", state["error"])
        self.assertNotIn("secret", json.dumps(state))

    def test_start_is_refused_without_setup(self):
        with patch.dict("os.environ", {"PURPLEAIR_API_KEY": ""}):
            response = self.client.post("/api/live/start")
            state = self.client.get("/api/live").get_json()

        self.assertEqual(response.status_code, 503)
        self.assertFalse(state["configured"])
        self.request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
