import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import azure.functions as func

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion_function"))
import function_app  # noqa: E402


class PushRequestLogTests(unittest.TestCase):
    def call(self, body, params=None, headers=None):
        table = MagicMock()
        service = MagicMock()
        service.get_table_client.return_value = table
        req = func.HttpRequest(
            method="POST",
            url="https://example.test/api/sensor_push",
            body=json.dumps(body).encode(),
            params=params or {},
            headers={"Content-Type": "application/json", "User-Agent": "PurpleAir/7.02", **(headers or {})},
        )
        env = {"AZURE_STORAGE_CONNECTION_STRING": "storage", "WEBHOOK_API_KEY": "secret"}
        with patch.dict("os.environ", env), \
                patch("azure.data.tables.TableServiceClient.from_connection_string", return_value=service), \
                patch("function_app.process_sensor_push", return_value={"sensor_id": "x"}):
            resp = function_app.sensor_push(req)
        return resp, table.create_entity.call_args.kwargs["entity"]

    def test_rejected_push_is_logged_without_secrets(self):
        resp, entry = self.call({"SensorId": "44:17:93:1:e8:e6", "api_key": "wrong"}, headers={"X-Api-Key": "wrong"})

        self.assertEqual(resp.status_code, 401)
        self.assertEqual(entry["status_code"], 401)
        self.assertEqual(entry["user_agent"], "PurpleAir/7.02")
        self.assertIn("x-api-key", entry["header_names"])
        self.assertIn("44:17:93:1:e8:e6", entry["body_preview"])
        self.assertNotIn("wrong", json.dumps(entry))

    def test_accepted_push_with_code_param_is_logged(self):
        resp, entry = self.call({"SensorId": "abc"}, params={"code": "secret"})

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(entry["query_keys"], "code")
        self.assertNotIn("secret", json.dumps(entry))


if __name__ == "__main__":
    unittest.main()
