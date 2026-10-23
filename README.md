# Healthy Gulf - PurpleAir Air Quality Monitoring

A cloud-native air quality ingestion and visualization pipeline built on Microsoft Azure. It monitors PurpleAir air quality sensors across the Gulf Coast region, ingests PM2.5, temperature, and humidity on a 5-minute schedule, and presents the data on an interactive map dashboard.

---

## 🏗️ Architecture

```mermaid
flowchart LR
    A[PurpleAir API\n/v1/sensors] -->|Batched GET| B(Azure Function\nTimer Trigger: 5 min)
    B -->|Active Sensors| C[(SensorRegistry\nTable)]
    B -->|Writes Readings| D[(PurpleAirData\nTable)]
    C -->|Coordinates & Names| E(Azure App Service\nFlask Dashboard)
    D -->|Latest Readings| E
    E -->|Interactive Map| F[Browser Dashboard\nLeaflet + OSM]
```

1. **Ingestion Engine (`ingestion_function/`)**:
   - Azure Function running Python 3.11 with a Timer Trigger (`0 */5 * * * *`, every 5 minutes).
   - Reads all active sensors from `SensorRegistry`.
   - Fetches current readings for all sensors in **a single batched PurpleAir API call** (`GET /v1/sensors?show_only=...`) to minimize API point consumption.
   - Saves new records into the `PurpleAirData` Azure Storage Table.

2. **Database (Azure Table Storage)**:
   - **`SensorRegistry` Table**: Holds sensor metadata (`PartitionKey="HealthyGulf"`, `RowKey=sensor_id`, `Name`, `Location`, `IsActive`, `Latitude`, `Longitude`).
   - **`PurpleAirData` Table**: Holds historical readings (`PartitionKey=sensor_id`, `RowKey=timestamp_sensor_id`, `pm2_5`, `temperature`, `humidity`, `timestamp`).

3. **Web Dashboard (`display_webapp/`)**:
   - Python Flask application hosted on Azure App Service.
   - Interactive Leaflet/OpenStreetMap map displaying sensor pins colored by US EPA AQI breakpoints (Good, Moderate, USG, Unhealthy, Very Unhealthy, Hazardous).
   - Clicking a pin opens a modal popup showing live PM2.5, temperature, humidity, last reported time, and a link to the official PurpleAir map.
   - Summary statistics cards and recent readings table with local timestamp conversion.

---

## 📍 Managing and Updating Sensors

Sensors can be added, updated, or removed using either the **CSV workflow** (recommended, tracked in git) or directly via the **Azure Portal GUI**.

### Method 1: CSV Workflow (Recommended)

The file [`sensors.csv`](./sensors.csv) acts as the version-controlled source of truth for your sensor list.

#### 1. Add or Edit Sensors
Open `sensors.csv` and add your sensors:
```csv
sensor_id,name,location,is_active
27589,Iberville,,true
98161,Irish Channel,,true
```

#### 2. Sync to Azure Table Storage
Run the sync script:
```powershell
# Add / update sensors from sensors.csv
python sync_sensor_registry.py

# To also remove any sensors from Azure that are NOT in sensors.csv:
python sync_sensor_registry.py --prune

# To remove deleted sensors AND back up & delete their historical readings:
python sync_sensor_registry.py --prune --delete-readings
```

#### 3. Backfill Coordinates (For New Sensors)
If you added new sensors, look up their latitude/longitude so they appear on the map:
```powershell
python backfill_sensor_locations.py
```
*Note: This script queries PurpleAir once for only the new sensors' coordinates and saves them to `SensorRegistry`.*

---

### Method 2: Azure Portal GUI (No Code Required)

1. Open the [Azure Portal](https://portal.azure.com/).
2. Navigate to **Storage accounts** $\rightarrow$ `healthygulfstor8842` $\rightarrow$ **Storage browser** $\rightarrow$ **Tables** $\rightarrow$ **`SensorRegistry`**.
3. **To add a sensor**:
   - Click **+ Add entity**.
   - Set `PartitionKey` = `HealthyGulf`.
   - Set `RowKey` = `<sensor_id>` (e.g. `27589`).
   - Add properties: `Name` (String), `IsActive` (Boolean = `true`), `Latitude` (Double), `Longitude` (Double).
4. **To temporarily pause a sensor**:
   - Select the sensor row, click **Edit**, and change `IsActive` to `false`. The ingestion function will automatically skip it on the next run.
5. **To delete a sensor**:
   - Select the checkbox next to the sensor and click **Delete**.

---

## 🧪 Local Development & Testing

All local operations connect securely to your Azure Storage using credentials stored in your local `.env` file (which is git-ignored).

### 1. Prerequisites & Installation
Ensure Python 3.11+ is installed, then install dependencies:
```powershell
pip install -r display_webapp/requirements.txt
pip install -r ingestion_function/requirements.txt
```

### 2. Test Ingestion Locally
To execute the exact same batched ingestion logic as the cloud Azure Function:
```powershell
python test_local_ingest.py
```
This fetches readings for all active sensors in `SensorRegistry` in 1 API call and writes them to Azure Table Storage.

### 3. Run the Dashboard Locally
The Events tab reads its historical readings from the `PurpleAirHistory` Azure Table. The Function App populates this table on an hourly schedule. For local Function runs, configure `PURPLEAIR_API_KEY`, `AZURE_STORAGE_CONNECTION_STRING`, `PURPLEAIR_API_CALLS_ENABLED=true`, and `PURPLEAIR_HISTORY_SYNC_ENABLED=true` in the Function App's local settings.

```powershell
cd display_webapp
python app.py
```
Open [http://localhost:8000](http://localhost:8000) in your web browser.

### 4. Configure Events History in Azure

1. Deploy the latest `main` branch so the Function App contains the scheduled history sync and the web app reads from `PurpleAirHistory`.
2. In Azure Portal, open the **Function App** `healthygulf-ingest-func` and go to **Settings → Environment variables → App settings**.
3. Set `PURPLEAIR_API_KEY` to a PurpleAir **READ** key and `AZURE_STORAGE_CONNECTION_STRING` to the storage account connection string used by the dashboard.
4. Set `PURPLEAIR_API_CALLS_ENABLED` to `true` and `PURPLEAIR_HISTORY_SYNC_ENABLED` to `true`. Save the settings and allow the Function App to restart.
5. In Azure Portal, open the **App Service** `healthygulf-dashboard-app` and confirm `AZURE_STORAGE_CONNECTION_STRING` points to the same storage account. Save if changed.
6. The history timer runs hourly, at the top of the hour. On its first successful run, it backfills seven days; later runs fetch from each sensor's checkpoint with a 20-minute overlap. The Function creates `PurpleAirHistory` and `PurpleAirHistorySyncState` automatically. Wait for the next hourly run before expecting historical events.
7. Open the dashboard's **Events** tab. It reads Azure Table Storage only; opening or refreshing the page does not call PurpleAir.

To pause only historical calls, set `PURPLEAIR_HISTORY_SYNC_ENABLED` to `false`; the existing five-minute current-reading ingestion continues. To pause **all** PurpleAir API calls, set `PURPLEAIR_API_CALLS_ENABLED` to `false`. To resume, set the relevant setting back to `true`. Azure restarts the Function App after saving app settings.

---

## 🚀 Deployment & CI/CD

Both services are deployed automatically via **GitHub Actions** workflows whenever you push changes to `main`:

- **Web App**: Triggered on changes to `display_webapp/**` via [`.github/workflows/main_healthygulf-dashboard-app.yml`](./.github/workflows/main_healthygulf-dashboard-app.yml).
- **Function App**: Triggered on changes to `ingestion_function/**` via [`.github/workflows/main_healthygulf-ingest-func.yml`](./.github/workflows/main_healthygulf-ingest-func.yml).

To deploy any changes:
```powershell
git add .
git commit -m "Update sensors and dashboard"
git push origin main
```

Live Dashboard URL: **`https://healthygulf-dashboard-app.azurewebsites.net`**

---

## ⏸️ Preserving API Points / Pausing Ingestion

When you want to pause data collection so you do not burn PurpleAir API tokens:

1. In the [Azure Portal](https://portal.azure.com/), open **Function App** $\rightarrow$ `healthygulf-ingest-func`.
2. On the **Overview** page, click **⏹️ Stop**.
3. To resume ingestion at any time, simply click **▶️ Start**.

*(Note: The web dashboard runs independently and will remain online and accessible even while the ingestion function is stopped.)*
