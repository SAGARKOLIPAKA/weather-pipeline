# 🌦️ Weather Data Pipeline

An end-to-end data engineering pipeline that ingests real-time weather data
from the OpenWeatherMap API, stores raw data in Azure Data Lake Storage Gen2,
transforms it using dbt, validates quality with Great Expectations, and
orchestrates everything with Apache Airflow — all running locally with Docker.

Built as a portfolio project covering both **ETL and ELT patterns** using
industry-standard tools.

**Live Dashboard →** [Global Weather Analytics Dashboard](https://public.tableau.com/app/profile/sagar.kolipaka7753/viz/GlobalWeatherAnalyticsDashboard/GlobalWeatherAnalyticsDashboard)

---

## Architecture

```
OpenWeatherMap API
        │
        ▼
Python Ingestion Script (fetch_weather.py)
        │
        ├─────────────────────────────────────┐
        ▼                                     ▼
Azure Data Lake Gen2                   PostgreSQL
(Bronze — raw JSON)              (raw_weather table)
date-partitioned by city                    │
                                            ▼
                                  Great Expectations
                                  (data quality gate)
                                            │
                                            ▼
                                      dbt Models
                                 ┌─────────────────┐
                                 │  stg_weather    │ ← Silver layer (view)
                                 │  daily_weather  │ ← Gold layer  (table)
                                 └─────────────────┘
                                            │
                                            ▼
                                    Tableau Public
                                  (Live Dashboard)
                                            │
                                 ┌──────────────────┐
                                 │   Airflow DAG    │
                                 │  orchestrates    │
                                 │  all steps on    │
                                 │ hourly schedule  │
                                 └──────────────────┘
```

---

## Medallion Architecture

| Layer | Location | Description |
|---|---|---|
| **Bronze** | Azure Data Lake Storage Gen2 | Raw JSON exactly as received from API, date-partitioned |
| **Silver** | PostgreSQL (`weather.stg_weather`) | Cleaned, typed, renamed columns — materialized as view |
| **Gold** | PostgreSQL (`weather.daily_weather`) | Daily aggregated analytics — materialized as table |

---

## Tech Stack

| Tool | Version | Purpose | Cloud Equivalent |
|---|---|---|---|
| Python | 3.11 | Data ingestion, scripting | — |
| Apache Airflow | 2.8.1 | Pipeline orchestration | AWS MWAA / Azure Data Factory |
| PostgreSQL | 15 | Data warehouse serving layer | AWS RDS / Azure DB for PostgreSQL |
| dbt | 1.7 | SQL transformations + testing | — |
| Azure Data Lake Gen2 | — | Raw data storage (Bronze) | AWS S3 |
| Great Expectations | 0.18 | Pre-load data quality validation | — |
| Docker + Compose | — | Local infrastructure | AWS ECS / Azure Container Instances |
| Tableau Public | — | Analytics dashboard | Power BI / Looker |

---

## Pipeline Flow

```
[1] fetch_weather        Pull current weather for 5 cities from OpenWeatherMap API
         ↓
[2] validate_data        Run Great Expectations checks on raw incoming data
         ↓
[3] load_to_postgres     Load raw JSON into PostgreSQL staging table
         ↓
[4] upload_to_azure      Upload raw JSON files to Azure Data Lake (Bronze layer)
         ↓
[5] dbt_run              Transform raw → Silver view → Gold analytics table
         ↓
[6] dbt_test             Validate transformed data with dbt schema tests
```

Each task depends on the previous — if any step fails, all downstream tasks
stop automatically. Airflow retries failed tasks once after 5 minutes.

---

## Project Structure

```
weather-pipeline/
│
├── ingestion/                        # Python scripts
│   ├── fetch_weather.py              # Pulls data from OpenWeatherMap API
│   ├── load_to_postgres.py           # Loads raw JSON into PostgreSQL
│   └── upload_to_azure.py            # Uploads files to Azure Data Lake
│
├── dbt_transforms/                   # dbt transformation layer
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_weather.sql       # Silver layer — clean + typed view
│   │   │   └── schema.yml            # Column definitions + dbt tests
│   │   └── marts/
│   │       └── daily_weather.sql     # Gold layer — daily aggregations table
│   └── dbt_project.yml               # dbt project config
│
├── airflow/
│   └── dags/
│       └── weather_dag.py            # 6-task Airflow DAG definition
│
├── data_quality/
│   └── expectations/
│       └── validate_weather.py       # Great Expectations validation suite
│
├── docker/
│   └── docker-compose.yml            # Airflow + PostgreSQL services
│
├── docs/
│   └── daily_weather.csv             # Gold layer export for Tableau
│
├── data/
│   └── raw/                          # Local Bronze landing zone (git-ignored)
│       └── YYYY-MM-DD/
│           └── city_HH-MM-SS.json
│
├── .env.example                      # Environment variable template
├── .gitignore                        # Protects secrets + ignores data files
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```

---

## Getting Started

### Prerequisites

- Docker Desktop
- Python 3.11+
- Git
- Azure Storage Account with hierarchical namespace enabled
- OpenWeatherMap API key (free at openweathermap.org)

### 1 — Clone the Repository

```bash
git clone https://github.com/SAGARKOLIPAKA/weather-pipeline.git
cd weather-pipeline
```

### 2 — Set Up Environment Variables

```bash
cp .env.example .env
# Open .env and fill in your real API keys and credentials
```

### 3 — Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4 — Start Local Infrastructure (Docker)

```bash
docker compose -f docker/docker-compose.yml up -d
```

- Airflow UI: http://localhost:8080 (username: `admin` / password: `admin`)
- PostgreSQL: `localhost:5432`

### 5 — Run the Pipeline Manually

```bash
# Step 1: Fetch weather data from API
python ingestion/fetch_weather.py

# Step 2: Validate raw data quality
python data_quality/expectations/validate_weather.py

# Step 3: Load into PostgreSQL
python ingestion/load_to_postgres.py

# Step 4: Upload to Azure Data Lake
python ingestion/upload_to_azure.py

# Step 5: Run dbt transformations
cd dbt_transforms && dbt run

# Step 6: Run dbt tests
dbt test && cd ..
```

### 6 — Or Let Airflow Orchestrate Everything

1. Go to http://localhost:8080
2. Find the `weather_pipeline` DAG
3. Toggle it **on** to enable hourly runs
4. Click **Trigger DAG** to run immediately

---

## dbt Models

### Silver Layer — `stg_weather` (View)

Reads from `raw_weather` table. Cleans column names, casts data types
explicitly, filters null records, and derives Fahrenheit temperature from
Celsius. Materialized as a **view** so it always reflects the latest data.

### Gold Layer — `daily_weather` (Table)

Reads from `stg_weather`. Aggregates daily statistics per city:
- Average, min, max temperature (°C and °F)
- Average, min, max humidity
- Average and max wind speed
- Dominant weather condition

Materialized as a **table** for fast query performance.

---

## Data Quality

Two independent validation layers:

### Great Expectations (Pre-Transformation Gate)
Validates raw data before it enters the transformation layer:
- `city` and `country` fields are never null
- Temperature within physical bounds (-90°C to 60°C)
- Humidity between 0–100%
- Wind speed non-negative (0–200 m/s)
- Minimum 1 record exists per run

### dbt Tests (Post-Transformation Validation)
Validates transformed Silver layer output:
- `weather_id` is unique and not null
- `city_name` is not null
- `temperature_c` is not null
- `humidity_pct` is not null

---

## Airflow DAG

**DAG ID:** `weather_pipeline`
**Schedule:** `@hourly`
**Owner:** sagar
**Retries:** 1 (5 minute delay)

```
fetch_weather → validate_data → load_to_postgres → upload_to_azure → dbt_run → dbt_test
```

---

## Environment Variables

See `.env.example` for all required variables:

| Variable | Description |
|---|---|
| `OPENWEATHER_API_KEY` | Free API key from openweathermap.org |
| `AZURE_STORAGE_ACCOUNT_NAME` | Azure Storage account name |
| `AZURE_STORAGE_ACCOUNT_KEY` | Azure Storage access key |
| `AZURE_CONTAINER_NAME` | Container name (default: `weather-raw`) |
| `POSTGRES_HOST` | PostgreSQL host (default: `localhost`) |
| `POSTGRES_PORT` | PostgreSQL port (default: `5432`) |
| `POSTGRES_DB` | Database name (default: `airflow`) |
| `POSTGRES_USER` | Database user (default: `airflow`) |
| `POSTGRES_PASSWORD` | Database password |

---

## Cities Tracked

| City | Country |
|---|---|
| London | 🇬🇧 GB |
| New York | 🇺🇸 US |
| Tokyo | 🇯🇵 JP |
| Sydney | 🇦🇺 AU |
| Mumbai | 🇮🇳 IN |

---

## AWS Equivalent Architecture

This pipeline was built on Azure but maps directly to AWS:

| Azure | AWS |
|---|---|
| Azure Data Lake Storage Gen2 | Amazon S3 |
| Azure Database for PostgreSQL | Amazon RDS |
| Azure Container Instances | Amazon ECS / Fargate |
| Azure Data Factory | AWS Glue / MWAA |

---

## Live Dashboard

🌍 [Global Weather Analytics Dashboard](https://public.tableau.com/app/profile/sagar.kolipaka7753/viz/GlobalWeatherAnalyticsDashboard/GlobalWeatherAnalyticsDashboard)

Built with Tableau Public. Visualizes the Gold layer output from the dbt pipeline —
showing temperature, humidity, wind speed, and weather conditions across 5 global cities.

---

## Future Enhancements

- [ ] Incremental loading — only process new records each run using watermarking
- [ ] CI/CD with GitHub Actions to run dbt tests on every pull request
- [ ] Multi-environment dbt profiles (dev / staging / prod)
- [ ] Deploy Airflow to Azure Container Instances for full cloud execution
- [ ] Extend to 20+ cities with config-driven city list
- [ ] Add data lineage visualization with dbt docs
- [ ] Real-time streaming layer with Azure Event Hub (Kafka equivalent)

---

## Author

**Sagar Kolipaka**
- GitHub: [@SAGARKOLIPAKA](https://github.com/SAGARKOLIPAKA)
- LinkedIn: [sagarkolipaka98](https://www.linkedin.com/in/sagarkolipaka98/)

---