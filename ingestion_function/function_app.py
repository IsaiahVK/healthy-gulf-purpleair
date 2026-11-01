import json
import logging
import os
import uuid
from datetime import datetime, timezone

import azure.functions as func

from ingestion import process_sensor_push, run_ingestion

app = func.FunctionApp()


def _purpleair_api_calls_enabled() -> bool:
    return os.environ.get("PURPLEAIR_API_CALLS_ENABLED", "true").strip().lower() == "true"

# ==============================================================================
# METHOD 1: Scheduled Pull (Runs every 5 minutes)
# ==============================================================================
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def purpleair_ingest(myTimer: func.TimerRequest) -> None:
    if not _purpleair_api_calls_enabled():
        logging.info("PurpleAir API calls are paused (PURPLEAIR_API_CALLS_ENABLED is not true).")
        return

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
PUSH_REQUEST_LOG_TABLE = "PushRequestLog"
# Never store these values, only note that they were sent
SECRET_NAMES = {"code", "api_key", "key", "x-functions-key", "x-api-key", "api-key", "authorization"}


def _push_request_log_enabled() -> bool:
    return os.environ.get("PUSH_REQUEST_LOG_ENABLED", "true").strip().lower() == "true"


def _redact_body(body: bytes) -> str:
    text = body[:1000].decode("utf-8", errors="replace")
    try:
        payload = json.loads(body)
    except Exception:
        return text
    if isinstance(payload, dict):
        payload = {k: ("<redacted>" if k.lower() in SECRET_NAMES else v) for k, v in payload.items()}
        return json.dumps(payload)[:1000]
    return text


def _log_push_request(req: func.HttpRequest, resp: func.HttpResponse) -> None:
    """Save who called /api/sensor_push and how we answered, so pushes can be debugged from storage."""
    if not _push_request_log_enabled():
        return
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        return
    try:
        from azure.core.exceptions import ResourceExistsError
        from azure.data.tables import TableServiceClient

        table = TableServiceClient.from_connection_string(conn_str=connection_string).get_table_client(PUSH_REQUEST_LOG_TABLE)
        try:
            table.create_table()
        except ResourceExistsError:
            pass
        now = datetime.now(timezone.utc)
        table.create_entity(entity={
            "PartitionKey": now.strftime("%Y%m%d"),
            "RowKey": f"{now.strftime('%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
            "received_at": now.isoformat(),
            "method": req.method,
            "status_code": resp.status_code,
            "response": resp.get_body()[:300].decode("utf-8", errors="replace"),
            "query_keys": ",".join(sorted(req.params.keys())),
            "header_names": ",".join(sorted(k.lower() for k in req.headers.keys())),
            "user_agent": req.headers.get("user-agent", ""),
            "content_type": req.headers.get("content-type", ""),
            "client_ip": (req.headers.get("x-forwarded-for") or "").split(",")[0].split(":")[0],
            "body_preview": _redact_body(req.get_body()),
        })
    except Exception:
        logging.exception("Could not write push request log")


@app.route(route="sensor_push", methods=["POST", "GET", "PUT"], auth_level=func.AuthLevel.ANONYMOUS)
def sensor_push(req: func.HttpRequest) -> func.HttpResponse:
    """Receives direct telemetry pushes from PurpleAir sensors configured with a Data Processor."""
    resp = _handle_sensor_push(req)
    _log_push_request(req, resp)
    return resp


def _handle_sensor_push(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "GET":
        return func.HttpResponse(
            json.dumps({"status": "ready", "message": "PurpleAir direct push endpoint is active"}),
            mimetype="application/json",
            status_code=200
        )

    logging.info(f"Received HTTP {req.method} push on /api/sensor_push")

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
