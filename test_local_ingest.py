import os
import requests
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from azure.data.tables import TableServiceClient

# Load .env file from project root
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

api_key = os.environ.get("PURPLEAIR_API_KEY")
connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

print("========================================")
print("🧪 Sensor Registry & Ingestion Local Test")
print("========================================")

if not api_key or not connection_string:
    print("❌ Error: Missing PURPLEAIR_API_KEY or AZURE_STORAGE_CONNECTION_STRING in .env")
    exit(1)

table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)

# 1. Connect to / Seed SensorRegistry table
registry_table_name = "SensorRegistry"
registry_client = table_service_client.get_table_client(table_name=registry_table_name)

try:
    registry_client.create_table()
    print(f"✅ Created '{registry_table_name}' table in Azure.")
except Exception:
    print(f"ℹ️  '{registry_table_name}' table already exists.")

# Check if any sensors exist in registry; if empty, seed with starter sensors!
existing_sensors = list(registry_client.list_entities())
if not existing_sensors:
    print("\n🌱 Seeding initial sensors into SensorRegistry...")
    starter_sensors = [
        {
            "PartitionKey": "HealthyGulf",
            "RowKey": "292843",
            "Name": "New Orleans Warehouse District",
            "Location": "New Orleans, LA",
            "IsActive": True
        },
        {
            "PartitionKey": "HealthyGulf",
            "RowKey": "307890",
            "Name": "JosephAQI",
            "Location": "New Orleans, LA",
            "IsActive": True
        }
    ]
    for s in starter_sensors:
        registry_client.upsert_entity(entity=s)
        print(f"   + Added sensor #{s['RowKey']} ({s['Name']})")
else:
    print(f"\n📋 Found {len(existing_sensors)} existing sensor(s) in '{registry_table_name}'.")

# 2. Query active sensors from registry
active_sensors = list(registry_client.query_entities("IsActive eq true"))
print(f"⚡ Active sensors to ingest: {len(active_sensors)}")

# 3. Destination Table: PurpleAirData
data_table_name = "PurpleAirData"
data_client = table_service_client.get_table_client(table_name=data_table_name)
try:
    data_client.create_table()
except Exception:
    pass

# 4. Fetch and Ingest each active sensor
headers = {"X-API-Key": api_key}
utc_now = datetime.now(timezone.utc)
row_key = utc_now.strftime("%Y%m%d%H%M%S")

print("\n🚀 Ingesting PurpleAir Data...")
for sensor in active_sensors:
    sensor_id = sensor.get("RowKey")
    registered_name = sensor.get("Name", "Unknown")
    
    url = f"https://api.purpleair.com/v1/sensors/{sensor_id}"
    print(f"   Fetching sensor #{sensor_id} ({registered_name})...")
    
    response = requests.get(url, headers=headers, timeout=15)
    if response.status_code == 200:
        sensor_data = response.json().get("sensor", {})
        entity = {
            "PartitionKey": str(sensor_id),
            "RowKey": f"{row_key}_{sensor_id}",
            "name": sensor_data.get("name", registered_name),
            "pm2_5": float(sensor_data.get("pm2.5", 0.0)),
            "temperature": float(sensor_data.get("temperature", 0.0)),
            "humidity": float(sensor_data.get("humidity", 0.0)),
            "timestamp": utc_now.isoformat()
        }
        data_client.upsert_entity(entity=entity)
        print(f"   ✅ Saved #{sensor_id}: PM2.5={entity['pm2_5']}, Temp={entity['temperature']}°F")
    else:
        print(f"   ❌ HTTP {response.status_code} for sensor #{sensor_id}")

print("\n🎉 Local ingestion test complete! All active sensors ingested into Azure.")
