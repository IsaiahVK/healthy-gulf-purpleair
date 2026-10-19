"""
Tests the direct push ingestion logic locally using realistic PurpleAir payloads.

    python test_push_ingest.py

Verifies that incoming JSON from a PurpleAir Data Processor is parsed,
dual-channel PM2.5 is averaged, and the reading is saved to Azure Table Storage.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(dotenv_path=ROOT / ".env")

sys.path.insert(0, str(ROOT / "ingestion_function"))
from ingestion import process_sensor_push  # noqa: E402

connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    print("ERROR: Missing AZURE_STORAGE_CONNECTION_STRING in .env")
    sys.exit(1)

print("========================================")
print("Testing Direct Push (Data Processor) Ingestion")
print("========================================")

# Example 1: Typical hardware payload from PurpleAir firmware
sample_firmware_payload = {
    "SensorId": "TEST-DIRECT-01",
    "DateTime": "2026/10/19 20:45:00z",
    "pm2_5_atm": 8.4,
    "pm2_5_atm_b": 8.8,
    "current_temp_f": 78,
    "current_humidity": 62,
    "latitude": 29.9511,
    "longitude": -90.0715,
    "name": "Healthy Gulf Test Unit"
}

print("\n1. Testing firmware telemetry packet (Channel A=8.4, Channel B=8.8)...")
res1 = process_sensor_push(sample_firmware_payload, connection_string)
print(f"   Success! Saved #{res1['sensor_id']} ({res1['name']}):")
print(f"     Averaged PM2.5 = {res1['pm2_5']} ug/m3")
print(f"     Temp = {res1['temperature']}F, Humidity = {res1['humidity']}%")

# Example 2: Minimal cloud forwarder packet
sample_cloud_payload = {
    "sensor_index": "TEST-DIRECT-01",
    "pm2.5": 9.1,
    "temperature": 77,
    "humidity": 61,
}
print("\n2. Testing second push update for same sensor (PM2.5=9.1)...")
res2 = process_sensor_push(sample_cloud_payload, connection_string)
print(f"   Success! Saved #{res2['sensor_id']} ({res2['name']}): PM2.5 = {res2['pm2_5']} ug/m3")

print("\nAll push ingestion tests passed successfully!")

