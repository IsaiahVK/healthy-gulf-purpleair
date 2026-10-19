# Healthy Gulf - PurpleAir Air Quality Monitoring

A cloud-native air quality ingestion and visualization pipeline built on Microsoft Azure. It monitors PurpleAir air quality sensors across the Gulf Coast region, supports both scheduled REST API polling and direct device telemetry push (webhooks), and presents the data on an interactive map dashboard.

---

## 🏗️ Architecture

```mermaid
flowchart TD
    subgraph Ingestion["Ingestion Layer (Azure Function App)"]
        subgraph ScheduledPull["Method 1: Scheduled Pull (API)"]
            A1[PurpleAir API\n/v1/sensors] -->|Batched GET\nEvery 5 min| B1[Timer Trigger\npurpleair_ingest]
        end
        subgraph DirectPush["Method 2: Direct Push (Hardware)"]
            A2[Owned PurpleAir Sensor\nData Processor] -->|HTTP POST\nReal-time Telemetry| B2[HTTP Trigger\n/api/sensor_push]
        end
    end

    subgraph Storage["Storage Layer (Azure Table Storage)"]
        C[(SensorRegistry\nTable)]
        D[(PurpleAirData\nTable)]
    end

    subgraph Frontend["Presentation Layer (Azure App Service)"]
        E(Flask Web App\nDashboard)
        F[Interactive Map\nLeaflet + OSM]
    end

    B1 -->|Query Active Sensors| C
    B1 -->|Save Readings| D
    B2 -->|Save Readings| D
    B2 -.->|Auto-update Coordinates| C

    C -->|Sensor Names & Coords| E
    D -->|Latest & Historical Data| E
    E --> F
```

1. **Dual Ingestion Engine (`ingestion_function/`)**:
   - **Method 1 (Scheduled Pull)**: Azure Function Timer Trigger (`0 */5 * * * *`, every 5 minutes). Reads active sensors from `SensorRegistry` and fetches their readings in **a single batched PurpleAir API call** (`GET /v1/sensors?show_only=...`).
   - **Method 2 (Direct Push / Data Processor)**: Azure Function HTTP Trigger (`POST /api/sensor_push`). Receives real-time telemetry pushed directly by registered PurpleAir sensors, averages dual-channel laser readings (Channels A & B), and writes them immediately to the database.

2. **Database (Azure Table Storage)**:
   - **`SensorRegistry` Table**: Holds sensor metadata (`PartitionKey="HealthyGulf"`, `RowKey=sensor_id`, `Name`, `Location`, `IsActive`, `Latitude`, `Longitude`, `IngestionType`).
   - **`PurpleAirData` Table**: Holds historical readings (`PartitionKey=sensor_id`, `RowKey=timestamp_sensor_id`, `pm2_5`, `temperature`, `humidity`, `timestamp`, `source`).

3. **Web Dashboard (`display_webapp/`)**:
   - Python Flask application hosted on Azure App Service.
   - Interactive Leaflet/OpenStreetMap map displaying sensor pins colored by US EPA AQI breakpoints (Good, Moderate, USG, Unhealthy, Very Unhealthy, Hazardous).
   - Clicking a pin displays live PM2.5, temperature, humidity, last reported time, and a link to the official PurpleAir map.
   - Automatically unifies both API-pulled and direct-push sensors on the same dashboard.

---

## 📡 Configuring Direct Push (PurpleAir Data Processor)

When registering a sensor that you own, you can configure it to push data directly to Azure using the sensor's **Data Processor** settings:

### Device Configuration Form
| Field | Value | Notes |
| :--- | :--- | :--- |
| **Provider** | `Microsoft Azure` | Or custom HTTP webhook |
| **API Key** | *Optional shared secret* | e.g. `healthygulf2026` (see Security below) |
| **Endpoint / ID** | `https://healthygulf-ingest-func.azurewebsites.net/api/sensor_push` | Your Azure Function HTTP route |
| **HTTP Method** | `POST` | Sensor transmits JSON telemetry |

### Security (API Key Authentication)
- If you set an API key in the PurpleAir Data Processor form, add an environment variable in your Azure Function App:
  - In Azure Portal $\rightarrow$ **Function App** $\rightarrow$ `healthygulf-ingest-func` $\rightarrow$ **Environment variables**.
  - Add `WEBHOOK_API_KEY` = `your-chosen-key`.
- If `WEBHOOK_API_KEY` is not set in Azure, the endpoint accepts pushes without requiring an API key.

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

### 2. Test Scheduled API Pull Locally
To execute the exact same batched pull logic as the scheduled Azure Function:
```powershell
python test_local_ingest.py
```

### 3. Test Direct Push (Webhook) Locally
To test the direct push payload parsing and database writing without needing the physical sensor online:
```powershell
python test_push_ingest.py
```

### 4. Run the Dashboard Locally
```powershell
cd display_webapp
python app.py
```
Open [http://localhost:8000](http://localhost:8000) in your web browser.

---

## 🚀 Deployment & CI/CD

Both services are deployed automatically via **GitHub Actions** workflows whenever you push changes to `main`:

- **Web App**: Triggered on changes to `display_webapp/**` via [`.github/workflows/main_healthygulf-dashboard-app.yml`](./.github/workflows/main_healthygulf-dashboard-app.yml).
- **Function App**: Triggered on changes to `ingestion_function/**` via [`.github/workflows/main_healthygulf-ingest-func.yml`](./.github/workflows/main_healthygulf-ingest-func.yml).

To deploy any changes:
```powershell
git add .
git commit -m "Add direct push webhook support for PurpleAir Data Processor"
git push origin main
```

Live Dashboard URL: **`https://healthygulf-dashboard-app.azurewebsites.net`**

---

## ⏸️ Preserving API Points / Pausing Ingestion

When you want to pause data collection so you do not burn PurpleAir API tokens:

1. In the [Azure Portal](https://portal.azure.com/), open **Function App** $\rightarrow$ `healthygulf-ingest-func`.
2. On the **Overview** page, click **⏹️ Stop**.
3. To resume ingestion at any time, simply click **▶️ Start**.

*(Note: Direct pushes also pause while the Function App is stopped. The web dashboard runs independently on App Service and remains online and accessible at all times.)*
