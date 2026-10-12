"""
Runs the exact same ingestion logic as the Azure Function, from your machine.

    python test_local_ingest.py

Uses ONE PurpleAir request for all active sensors in SensorRegistry and writes the
readings to the real Azure Table Storage (so they show up on the dashboard).
"""
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=ROOT / ".env")

# Import the shared module the Azure Function uses
sys.path.insert(0, str(ROOT / "ingestion_function"))
from ingestion import run_ingestion  # noqa: E402

logging.basicConfig(level=logging.INFO, format="   %(message)s")
logging.getLogger("azure").setLevel(logging.WARNING)  # hide noisy Azure SDK HTTP logs

api_key = os.environ.get("PURPLEAIR_API_KEY")
connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

print("========================================")
print("Local Ingestion Test (same code as the Azure Function)")
print("========================================")

if not api_key or not connection_string:
    print("ERROR: Missing PURPLEAIR_API_KEY or AZURE_STORAGE_CONNECTION_STRING in .env")
    sys.exit(1)

result = run_ingestion(api_key, connection_string)

if result["requested"] == 0:
    print("\nERROR: No active sensors. Run `python sync_sensor_registry.py` to load sensors.csv first.")
    sys.exit(1)

print(f"\nDone: saved {result['saved']} of {result['requested']} sensor(s) using 1 PurpleAir request.")
if result["missing"]:
    print(f"WARNING: Not returned by PurpleAir: {', '.join(result['missing'])}")
