"""
fetch_weather.py
----------------
Pulls current weather data from OpenWeatherMap API for a list of cities.
Saves raw JSON responses to local files, partitioned by date and city.

Bronze Layer principle: data is saved exactly as received — no modifications.
Transformations happen downstream in dbt.
"""

import os
import json
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv

# ─────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────

# Load environment variables from .env file
# This is how we keep secrets out of the code
load_dotenv()

# Configure logging
# Every pipeline run should produce logs — this is how you debug failures
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

# Read API key from environment — never hardcode this
API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Cities we want to track
# In a real pipeline, this might come from a config file or database
CITIES = [
    "London",
    "New York",
    "Tokyo",
    "Sydney",
    "Mumbai"
]

# Where to save raw data locally before uploading to Azure
RAW_OUTPUT_DIR = "data/raw"


# ─────────────────────────────────────────
# FUNCTIONS
# ─────────────────────────────────────────

def fetch_weather(city: str) -> dict:
    """
    Fetch current weather data for a single city.

    Args:
        city: Name of the city to fetch weather for

    Returns:
        Dictionary containing raw API response

    Raises:
        Exception if API call fails
    """
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"      # Celsius; use "imperial" for Fahrenheit
    }

    logger.info(f"Fetching weather for: {city}")

    response = requests.get(BASE_URL, params=params, timeout=10)

    # Raise an error if the request failed (4xx or 5xx status codes)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Successfully fetched data for {city} — temp: {data['main']['temp']}°C")

    return data


def save_raw_data(data: dict, city: str, timestamp: str) -> str:
    """
    Save raw API response as JSON file.
    Path pattern: data/raw/YYYY-MM-DD/city_HH-MM-SS.json

    This partitioning by date is the same pattern used in
    Azure Data Lake and AWS S3 for efficient querying.

    Args:
        data: Raw API response dictionary
        city: City name (used in filename)
        timestamp: Current timestamp string

    Returns:
        Path where file was saved
    """
    # Create date partition folder (e.g. data/raw/2026-05-12)
    date_partition = datetime.utcnow().strftime("%Y-%m-%d")
    output_dir = os.path.join(RAW_OUTPUT_DIR, date_partition)
    os.makedirs(output_dir, exist_ok=True)

    # Clean city name for filename (replace spaces with underscores)
    city_clean = city.replace(" ", "_").lower()
    filename = f"{city_clean}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Saved raw data to: {filepath}")
    return filepath


def run_ingestion():
    """
    Main ingestion function.
    Loops through all cities, fetches weather data, saves locally.
    """
    # Validate API key exists before making any calls
    if not API_KEY:
        logger.error("OPENWEATHER_API_KEY not found in environment variables")
        logger.error("Make sure your .env file exists and contains the key")
        raise ValueError("Missing API key")

    # Timestamp for this run — same timestamp used across all cities
    # so we know they were all fetched in the same pipeline run
    run_timestamp = datetime.utcnow().strftime("%H-%M-%S")

    logger.info("=" * 50)
    logger.info(f"Starting weather ingestion run at {run_timestamp} UTC")
    logger.info(f"Cities to fetch: {CITIES}")
    logger.info("=" * 50)

    successful = []
    failed = []

    for city in CITIES:
        try:
            # Fetch data from API
            raw_data = fetch_weather(city)

            # Add ingestion metadata to the raw record
            # This is useful for debugging and auditing later
            raw_data["_ingested_at"] = datetime.utcnow().isoformat()
            raw_data["_source"] = "openweathermap"

            # Save locally
            filepath = save_raw_data(raw_data, city, run_timestamp)
            successful.append(city)

        except requests.exceptions.HTTPError as e:
            # API returned an error (e.g. 401 invalid key, 404 city not found)
            logger.error(f"HTTP error for {city}: {e}")
            failed.append(city)

        except requests.exceptions.ConnectionError:
            # No internet connection or API is down
            logger.error(f"Connection error for {city} — check internet connection")
            failed.append(city)

        except requests.exceptions.Timeout:
            # API took too long to respond
            logger.error(f"Timeout for {city} — API too slow to respond")
            failed.append(city)

        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"Unexpected error for {city}: {e}")
            failed.append(city)

    # ── Summary ──
    logger.info("=" * 50)
    logger.info(f"Ingestion complete.")
    logger.info(f"Successful: {len(successful)} cities — {successful}")
    if failed:
        logger.warning(f"Failed: {len(failed)} cities — {failed}")
    logger.info("=" * 50)


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":
    run_ingestion()