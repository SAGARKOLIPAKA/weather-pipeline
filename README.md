# Weather Data Pipeline 🌦️

An end-to-end data engineering pipeline that ingests real-time weather data
from the OpenWeatherMap API, stores it in Azure Data Lake Storage Gen2,
transforms it using dbt, and orchestrates everything with Apache Airflow.

Built as a portfolio project covering both ETL and ELT patterns using
industry-standard tools.

---

## Architecture
### Medallion Architecture
| Layer | Location | Description |
|---|---|---|
| **Bronze** | Azure Data Lake Storage Gen2 | Raw JSON exactly as received from API |
| **Silver** | PostgreSQL (`weather.stg_weather`) | Cleaned, typed, renamed columns |
| **Gold** | PostgreSQL (`weather.daily_weather`) | Daily aggregated analytics table |

---

## Tech Stack

| Tool | Purpose | Cloud Equivalent |
|---|---|---|
| Python 3.11 | Data ingestion, scripting | — |
| Apache Airflow 2.8 | Pipeline orchestration | AWS MWAA / Azure Data Factory |
| PostgreSQL 15 | Data warehouse (serving layer) | AWS RDS / Azure Database for PostgreSQL |
| dbt (data build tool) | SQL transformations + testing | — |
| Azure Data Lake Gen2 | Raw data storage (Bronze layer) | AWS S3 |
| Great Expectations | Data quality validation | — |
| Docker + Compose | Local infrastructure | AWS ECS / Azure Container Instances |

---

## Pipeline Flow
Each step depends on the previous — if any step fails, downstream tasks
stop automatically and Airflow retries once after 5 minutes.

---

## Project Structure
---

## Getting Started

### Prerequisites
- Docker Desktop
- Python 3.11+
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
# Edit .env with your real API keys and credentials
```

### 3 — Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 4 — Start Local Infrastructure
```bash
docker compose -f docker/docker-compose.yml up -d
```

Airflow UI available at: http://localhost:8080 (admin/admin)

### 5 — Run the Pipeline Manually
```bash
# Fetch weather data
python ingestion/fetch_weather.py

# Validate data quality
python data_quality/expectations/validate_weather.py

# Load into PostgreSQL
python ingestion/load_to_postgres.py

# Upload to Azure Data Lake
python ingestion/upload_to_azure.py

# Run dbt transformations
cd dbt_transforms && dbt run && dbt test
```

### 6 — Or Let Airflow Orchestrate It
Enable the `weather_pipeline` DAG in the Airflow UI.
It runs automatically every hour.

---

## dbt Models

### Silver Layer — `stg_weather`
Reads from `raw_weather` table. Cleans column names, casts data types,
filters nulls, and derives Fahrenheit temperature from Celsius.
Materialized as a **view** for freshness.

### Gold Layer — `daily_weather`
Reads from `stg_weather`. Aggregates daily statistics per city:
average/min/max temperature, humidity ranges, wind speed,
and dominant weather condition.
Materialized as a **table** for query performance.

---

## Data Quality

Two layers of validation:

**Great Expectations** (pre-transformation):
- City and country fields never null
- Temperature within physical bounds (-90°C to 60°C)
- Humidity between 0–100%
- Wind speed non-negative
- Minimum 1 record per run

**dbt tests** (post-transformation):
- `weather_id` is unique and not null
- `city_name` is not null
- `temperature_c` is not null
- `humidity_pct` is not null

---

## Environment Variables

See `.env.example` for all required variables:

| Variable | Description |
|---|---|
| `OPENWEATHER_API_KEY` | OpenWeatherMap API key |
| `AZURE_STORAGE_ACCOUNT_NAME` | Azure Storage account name |
| `AZURE_STORAGE_ACCOUNT_KEY` | Azure Storage access key |
| `AZURE_CONTAINER_NAME` | Azure container name (default: weather-raw) |
| `POSTGRES_HOST` | PostgreSQL host (default: localhost) |
| `POSTGRES_PORT` | PostgreSQL port (default: 5432) |
| `POSTGRES_DB` | Database name |
| `POSTGRES_USER` | Database user |
| `POSTGRES_PASSWORD` | Database password |

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

## Author

**Sagar Kolipaka**
- GitHub: [@SAGARKOLIPAKA](https://github.com/SAGARKOLIPAKA)
- LinkedIn: *(add your LinkedIn URL)*
