import azure.functions as func
import logging
import os

from ingestion import run_history_sync, run_ingestion

app = func.FunctionApp()


def _purpleair_api_calls_enabled() -> bool:
    return os.environ.get("PURPLEAIR_API_CALLS_ENABLED", "true").strip().lower() == "true"

# Runs every 5 minutes: "0 */5 * * * *"
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def purpleair_ingest(myTimer: func.TimerRequest) -> None:
    if not _purpleair_api_calls_enabled():
        logging.info("PurpleAir API calls are paused (PURPLEAIR_API_CALLS_ENABLED is not true).")
        return

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


@app.timer_trigger(schedule="0 0 * * * *", arg_name="historyTimer", run_on_startup=False, use_monitor=True)
def purpleair_history_sync(historyTimer: func.TimerRequest) -> None:
    if not _purpleair_api_calls_enabled():
        logging.info("PurpleAir API calls are paused (PURPLEAIR_API_CALLS_ENABLED is not true).")
        return
    if os.environ.get("PURPLEAIR_HISTORY_SYNC_ENABLED", "false").strip().lower() != "true":
        logging.info("PurpleAir history sync is paused (PURPLEAIR_HISTORY_SYNC_ENABLED is not true).")
        return

    if historyTimer.past_due:
        logging.info("PurpleAir history sync timer is past due.")

    api_key = os.environ.get("PURPLEAIR_API_KEY")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not api_key or not connection_string:
        logging.error("History sync requires PURPLEAIR_API_KEY and AZURE_STORAGE_CONNECTION_STRING.")
        return

    try:
        run_history_sync(api_key, connection_string, logger=logging)
    except Exception:
        logging.exception("PurpleAir history sync failed.")
