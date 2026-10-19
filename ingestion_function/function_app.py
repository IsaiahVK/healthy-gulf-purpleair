import json
import logging
import os
import azure.functions as func

from ingestion import run_ingestion, process_sensor_push

app = func.FunctionApp()


# ==============================================================================
# METHOD 1: Scheduled Pull (Runs every 5 minutes)
# ==============================================================================
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def purpleair_ingest(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executing (PurpleAir API Pull)...')

    api_key = os.environ.get("PURPLEAIR_API_KEY")
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

    if not api_key:
        logging.error("Missing PURPLEAIR_API_KEY environment variable.")
        return

    if not connection_string:
        logging.error("Missing AZURE_STORAGE_CONNECTION_STRING environment variable.")
        return

    try:
        run_ingestion(api_key, connection_string, logger=logging)
    except Exception as e:
        logging.error(f"Scheduled API ingestion failed: {str(e)}")


# ==============================================================================
# METHOD 2: Direct Push (HTTP Webhook for PurpleAir Data Processor)
# ==============================================================================
@app.route(route="sensor_push", methods=["POST", "GET"], auth_level=func.AuthLevel.ANONYMOUS)
def sensor_push(req: func.HttpRequest) -> func.HttpResponse:
    """Receives direct telemetry pushes from PurpleAir sensors configured with a Data Processor."""
    if req.method == "GET":
        return func.HttpResponse(
            json.dumps({"status": "ready", "message": "PurpleAir direct push endpoint is active"}),
            mimetype="application/json",
            status_code=200
        )

    logging.info("Received HTTP POST push on /api/sensor_push")

    # Optional API Key Authentication
    expected_key = os.environ.get("WEBHOOK_API_KEY")
    if expected_key:
        auth_header = req.headers.get("Authorization", "")
        received_key = (
            req.headers.get("x-functions-key")
            or req.headers.get("x-api-key")
            or req.headers.get("api-key")
            or req.params.get("code")
            or req.params.get("api_key")
        )
        if not received_key and auth_header:
            if auth_header.lower().startswith("bearer ") or auth_header.lower().startswith("apikey "):
                received_key = auth_header.split(" ", 1)[1].strip()
            else:
                received_key = auth_header.strip()

        payload_obj = None
        try:
            payload_obj = req.get_json()
            if isinstance(payload_obj, dict) and not received_key:
                received_key = payload_obj.get("api_key") or payload_obj.get("key")
        except Exception:
            pass

        if received_key != expected_key:
            logging.warning("Unauthorized sensor push attempt: Invalid or missing API key.")
            return func.HttpResponse(
                json.dumps({"error": "Unauthorized: Invalid API key"}),
                mimetype="application/json",
                status_code=401
            )
    else:
        try:
            payload_obj = req.get_json()
        except Exception:
            payload_obj = None

    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        logging.error("Missing AZURE_STORAGE_CONNECTION_STRING environment variable.")
        return func.HttpResponse(
            json.dumps({"error": "Server configuration error: missing storage connection string"}),
            mimetype="application/json",
            status_code=500
        )

    if not payload_obj:
        try:
            raw_body = req.get_body().decode("utf-8")
            logging.info(f"Raw push body received: {raw_body[:300]}")
            payload_obj = json.loads(raw_body)
        except Exception as e:
            return func.HttpResponse(
                json.dumps({"error": f"Invalid payload: expected JSON ({str(e)})"}),
                mimetype="application/json",
                status_code=400
            )

    try:
        result = process_sensor_push(payload_obj, connection_string, logger=logging)
        return func.HttpResponse(
            json.dumps({"status": "success", "result": result}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.exception(f"Error processing sensor push: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Internal processing error: {str(e)}"}),
            mimetype="application/json",
            status_code=500
        )
