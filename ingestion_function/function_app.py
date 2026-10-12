import azure.functions as func
import logging
import os

from ingestion import run_ingestion

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

    # All active sensors in SensorRegistry are fetched in ONE PurpleAir request (see ingestion.py)
    try:
        run_ingestion(api_key, connection_string, logger=logging)
    except Exception as e:
        logging.error(f"Ingestion failed: {str(e)}")
