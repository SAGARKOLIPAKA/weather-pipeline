"""
load_to_postgres.py
-------------------
Loads raw weather JSON files from local storage into
PostgreSQL as a raw staging table.

This bridges the gap between local Bronze files and
dbt transformations — dbt reads from PostgreSQL, not
directly from Azure or local JSON files.
"""

import os
import json
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

LOCAL_RAW_DIR = "data/raw"

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     os.getenv("POSTGRES_PORT", 5432),
    "dbname":   os.getenv("POSTGRES_DB", "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow")
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_raw_table(cursor):
    """
    Create the raw weather table if it doesn't exist.
    This is the landing zone for raw JSON data in PostgreSQL.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_weather (
            id              SERIAL PRIMARY KEY,
            city            VARCHAR(100),
            country         VARCHAR(10),
            temperature     NUMERIC(5,2),
            feels_like      NUMERIC(5,2),
            humidity        INTEGER,
            pressure        INTEGER,
            wind_speed      NUMERIC(5,2),
            weather_main    VARCHAR(50),
            weather_desc    VARCHAR(100),
            ingested_at     TIMESTAMP,
            source          VARCHAR(50),
            raw_json        JSONB
        )
    """)
    logger.info("Raw weather table ready")


def parse_weather_record(data: dict) -> dict:
    """
    Extract key fields from raw OpenWeatherMap JSON.
    Store the full JSON too — never throw away raw data.
    """
    return {
        "city":         data.get("name"),
        "country":      data.get("sys", {}).get("country"),
        "temperature":  data.get("main", {}).get("temp"),
        "feels_like":   data.get("main", {}).get("feels_like"),
        "humidity":     data.get("main", {}).get("humidity"),
        "pressure":     data.get("main", {}).get("pressure"),
        "wind_speed":   data.get("wind", {}).get("speed"),
        "weather_main": data.get("weather", [{}])[0].get("main"),
        "weather_desc": data.get("weather", [{}])[0].get("description"),
        "ingested_at":  data.get("_ingested_at", datetime.utcnow().isoformat()),
        "source":       data.get("_source", "openweathermap"),
        "raw_json":     json.dumps(data)
    }


def load_files_to_postgres():
    """
    Read all local JSON files and insert into PostgreSQL raw table.
    """
    logger.info("=" * 50)
    logger.info("Starting PostgreSQL load")
    logger.info("=" * 50)

    conn   = get_connection()
    cursor = conn.cursor()

    create_raw_table(cursor)
    conn.commit()

    loaded = 0
    fa