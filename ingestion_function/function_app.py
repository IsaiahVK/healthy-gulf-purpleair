import azure.functions as func
import logging
import requests
import os
from datetime import datetime, timezone
from azure.data.tables import TableServiceClient

app = func.FunctionApp()

# Runs every 5 minutes: "0 */5 * * * *"
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def purpleair_ingest(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executing...')
    
    api_key = os.environ.get("PURPLEAIR_API_KEY")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    
    if not api_key:
        logging.error("Missing PURPLEAIR_API_KEY environment variable.")
        return
        
    if not connection_string:
        logging.error("Missing AZURE_STORAGE_CONNECTION_STRING environment variable.")
        return

    table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
    
    # 1. Connect to SensorRegistry table
    registry_table_name = "SensorRegistry"
    registry_client = table_service_client.get_table_client(table_name=registry_table_name)
    
    try:
        registry_client.create_table()
    except Exception:
        pass

    # 2. Query all active sensors
    try:
        active_sensors = list(registry_client.query_entities("IsActive eq true"))
    except Exception as e:
        logging.error(f"Error querying SensorRegistry: {str(e)}")
        return

    if not active_sensors:
        logging.warning("No active sensors found in SensorRegistry table. Nothing to ingest.")
        return

    logging.info(f"Found {len(active_sensors)} active sensor(s) to ingest.")

    # 3. Connect to PurpleAirData destination table
    data_table_name = "PurpleAirData"
    data_client = table_service_client.get_table_client(table_name=data_table_name)
    try:
        data_client.create_table()
    except Exception:
        pass

    # 4. Fetch and save data for each active sensor
    headers = {"X-API-Key": api_key}
    utc_now = datetime.now(timezone.utc)
    row_key = utc_now.strftime("%Y%m%d%H%M%S")

    for sensor in active_sensors:
        sensor_index = sensor.get("RowKey")
        sensor_name = sensor.get("Name", "Unknown")

        if not sensor_index:
            continue

        url = f"https://api.purpleair.com/v1/sensors/{sensor_index}"
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                sensor_data = data.get("sensor", {})
                
                entity = {
                    "PartitionKey": str(sensor_index),
                    "RowKey": f"{row_key}_{sensor_index}",
                    "name": sensor_data.get("name", sensor_name),
                    "pm2_5": float(sensor_data.get("pm2.5", 0.0)),
                    "temperature": float(sensor_data.get("temperature", 0.0)),
                    "humidity": float(sensor_data.get("humidity", 0.0)),
                    "timestamp": utc_now.isoformat()
                }
                
                data_client.upsert_entity(entity=entity)
                logging.info(f"Successfully ingested sensor {sensor_index} ({entity['name']})")
            else:
                logging.error(f"Failed to fetch sensor {sensor_index}: HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"Error fetching sensor {sensor_index}: {str(e)}")
