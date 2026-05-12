"""
upload_to_azure.py
------------------
Uploads raw weather JSON files from local storage to
Azure Data Lake Storage Gen2 (Bronze layer).

Design principle:
- Local files are the staging area
- Azure is the permanent Bronze layer
- Files are uploaded with the same date partitioning
  used locally, preserving the folder structure in the cloud
"""

import os
import logging
from datetime import datetime

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# ─────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER    = os.getenv("AZURE_CONTAINER_NAME", "weather-raw")
LOCAL_RAW_DIR = "data/raw"


# ─────────────────────────────────────────
# FUNCTIONS
# ─────────────────────────────────────────

def get_blob_client():
    """
    Create and return an Azure BlobServiceClient.

    BlobServiceClient is the top-level client for interacting
    with your entire storage account.
    """
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(connection_string)


def upload_file(blob_service_client, local_path: str, blob_path: str):
    """
    Upload a single file to Azure Data Lake.

    Args:
        blob_service_client: Authenticated Azure client
        local_path: Path to local file e.g. data/raw/2026-05-12/london.json
        blob_path:  Path in Azure e.g. bronze/2026-05-12/london.json
    """
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER,
        blob=blob_path
    )

    with open(local_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    logger.info(f"Uploaded: {local_path} → azure://{CONTAINER}/{blob_path}")


def upload_all_raw_files():
    """
    Walk through all local raw JSON files and upload to Azure.

    Folder structure in Azure mirrors local structure:
    Local:  data/raw/2026-05-12/london.json
    Azure:  bronze/2026-05-12/london.json

    Why 'bronze/' prefix in Azure?
    Naming the top-level folder 'bronze' makes the
    Medallion architecture explicit in the data lake itself.
    When we add Silver and Gold later, they get their own
    top-level folders too.
    """

    # Validate credentials exist
    if not ACCOUNT_NAME or not ACCOUNT_KEY:
        logger.error("Azure credentials missing from .env file")
        raise ValueError("Missing Azure credentials")

    logger.info("=" * 50)
    logger.info("Starting Azure upload run")
    logger.info(f"Source: {LOCAL_RAW_DIR}")
    logger.info(f"Destination: azure://{CONTAINER}/bronze/")
    logger.info("=" * 50)

    # Connect to Azure
    blob_service_client = get_blob_client()

    uploaded = []
    failed   = []

    # Walk through every file in data/raw/
    for date_folder in os.listdir(LOCAL_RAW_DIR):
        date_path = os.path.join(LOCAL_RAW_DIR, date_folder)

        # Skip if not a folder
        if not os.path.isdir(date_path):
            continue

        for filename in os.listdir(date_path):
            if not filename.endswith(".json"):
                continue

            local_path = os.path.join(date_path, filename)

            # Mirror the local path inside bronze/ folder in Azure
            # Local:  data/raw/2026-05-12/london.json
            # Azure:  bronze/2026-05-12/london.json
            blob_path = f"bronze/{date_folder}/{filename}"

            try:
                upload_file(blob_service_client, local_path, blob_path)
                uploaded.append(blob_path)

            except Exception as e:
                logger.error(f"Failed to upload {filename}: {e}")
                failed.append(filename)

    # ── Summary ──
    logger.info("=" * 50)
    logger.info(f"Upload complete.")
    logger.info(f"Uploaded: {len(uploaded)} files")
    if failed:
        logger.warning(f"Failed: {len(failed)} files — {failed}")
    logger.info("=" * 50)


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":
    upload_all_raw_files()