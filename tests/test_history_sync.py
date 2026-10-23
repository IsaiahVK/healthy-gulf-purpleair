import unittest
import importlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

from ingestion_function.ingestion import run_history_sync

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion_function"))


class HistorySyncTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
        self.reading_time = datetime(2026, 10, 22, 11, 50, tzinfo=timezone.utc)
        self.registry = MagicMock()
        self.registry.query_entities.return_value = [
            {"RowKey": "123", "Name": "Test sensor", "IsActive": True}
        ]
        self.history = MagicMock()
        self.state = MagicMock()
        self.service = MagicMock()
        self.service.get_table_client.side_effect = lambda table_name: {
            "SensorRegistry": self.registry,
            "PurpleAirHistory": self.history,
            "PurpleAirHistorySyncState": self.state,
        }[table_name]
        self.response = MagicMock()
        self.response.json.return_value = {
            "fields": ["sensor_index", "time_stamp", "pm2.5_atm"],
            "data": [[123, int(self.reading_time.timestamp()), 42.5]],
        }

    def run_sync(self):
        with patch("ingestion_function.ingestion.TableServiceClient.from_connection_string", return_value=self.service), \
                patch("ingestion_function.ingestion.requests.get", return_value=self.response) as request, \
                patch("ingestion_function.ingestion.time.monotonic", return_value=0), \
                patch("ingestion_function.ingestion.time.sleep") as sleep:
            result = run_history_sync("read-key", "storage", now=self.now)
        return result, request, sleep

    def test_initial_sync_backfills_seven_days_and_saves_readings(self):
        self.state.get_entity.side_effect = ResourceNotFoundError("not initialized")

        result, request, _ = self.run_sync()

        self.assertEqual(request.call_args.kwargs["params"]["start_timestamp"], int((self.now - timedelta(days=7)).timestamp()))
        self.assertEqual(request.call_args.kwargs["params"]["average"], 10)
        self.assertEqual(result, {"requested": 1, "saved": 1, "failed": []})
        operations = self.history.submit_transaction.call_args.args[0]
        entity = operations[0][1]
        self.assertEqual(entity["PartitionKey"], "123")
        self.assertEqual(entity["RowKey"], self.reading_time.strftime("%Y%m%d%H%M%S"))
        self.assertEqual(entity["pm2_5"], 42.5)

    def test_incremental_sync_uses_checkpoint_with_overlap(self):
        checkpoint_time = self.now - timedelta(hours=2)
        self.state.get_entity.return_value = {"last_timestamp": checkpoint_time.isoformat()}

        _, request, _ = self.run_sync()

        expected_start = checkpoint_time - timedelta(minutes=20)
        self.assertEqual(request.call_args.kwargs["params"]["start_timestamp"], int(expected_start.timestamp()))

    def test_existing_tables_are_reused_and_readings_are_upserted(self):
        self.state.get_entity.side_effect = ResourceNotFoundError("not initialized")
        self.history.create_table.side_effect = ResourceExistsError("exists")
        self.state.create_table.side_effect = ResourceExistsError("exists")

        _, _, sleep = self.run_sync()

        self.history.submit_transaction.assert_called_once()
        self.assertEqual(self.history.submit_transaction.call_args.args[0][0][0], "upsert")
        self.state.upsert_entity.assert_called_once()
        sleep.assert_not_called()

    def test_master_pause_disables_both_purpleair_timers(self):
        timer = MagicMock(past_due=False)
        fake_functions = ModuleType("azure.functions")

        class FakeFunctionApp:
            def timer_trigger(self, **kwargs):
                return lambda handler: handler

        fake_functions.FunctionApp = FakeFunctionApp
        fake_functions.TimerRequest = object
        with patch.dict(sys.modules, {"azure.functions": fake_functions}):
            function_app = importlib.import_module("function_app")
            with patch.dict("os.environ", {
                "PURPLEAIR_API_CALLS_ENABLED": "false",
                "PURPLEAIR_HISTORY_SYNC_ENABLED": "true",
            }), patch.object(function_app, "run_ingestion") as current_ingest, \
                    patch.object(function_app, "run_history_sync") as history_sync:
                function_app.purpleair_ingest(timer)
                function_app.purpleair_history_sync(timer)
            sys.modules.pop("function_app", None)

        current_ingest.assert_not_called()
        history_sync.assert_not_called()


if __name__ == "__main__":
    unittest.main()
