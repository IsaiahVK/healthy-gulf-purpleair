from flask import Flask, render_template
import os
from pathlib import Path
from dotenv import load_dotenv
from azure.data.tables import TableServiceClient

# Load environment variables from .env in project root if present
root_env = Path(__file__).resolve().parent.parent / ".env"
if root_env.exists():
    load_dotenv(dotenv_path=root_env)
else:
    load_dotenv()

app = Flask(__name__)

@app.route('/')
def index():
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        return "ERROR: AZURE_STORAGE_CONNECTION_STRING environment variable is not set.", 500
        
    try:
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
        table_client = table_service_client.get_table_client(table_name="PurpleAirData")
        
        # Fetch entities from Table Storage.
        entities = list(table_client.list_entities())
        
        # Sort descending by RowKey (timestamp string YYYYMMDDHHMMSS)
        entities.sort(key=lambda x: x.get('RowKey', ''), reverse=True)
        recent_data = entities[:50]
        
        return render_template('index.html', data=recent_data)
    except Exception as e:
        return f"Error fetching data from Azure: {str(e)}", 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting dashboard at http://localhost:{port}...")
    app.run(host='0.0.0.0', port=port, debug=True)
