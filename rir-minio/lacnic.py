
import os
import gzip
import logging
import tempfile
from pathlib import Path

import requests
from minio import Minio


# --------------------------------------------------
# Configuration
# --------------------------------------------------

LACNIC_URL = "https://ftp.lacnic.net/lacnic/dbase/lacnic.db.gz"
BUCKET_NAME = "lacnic"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# HTTP Session
# --------------------------------------------------

session = requests.Session()
session.headers.update({
    "User-Agent": "LACNIC-Minio-Downloader/1.0"
})


# --------------------------------------------------
# MinIO Setup
# --------------------------------------------------

def create_minio_client():

    if not MINIO_ENDPOINT:
        raise ValueError("MINIO_ENDPOINT is missing.")

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise ValueError(
            "Missing MinIO credentials. Set MINIO_ROOT_USER "
            "and MINIO_ROOT_PASSWORD before running the script."
        )

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


# --------------------------------------------------
# Check MinIO Bucket
# --------------------------------------------------

def ensure_bucket_exists(client):

    if not client.bucket_exists(BUCKET_NAME):
        raise RuntimeError(
            f"MinIO bucket '{BUCKET_NAME}' does not exist. "
            "Create it before running this script."
        )

    logger.info("MinIO bucket '%s' is available.", BUCKET_NAME)


# --------------------------------------------------
# Download File
# --------------------------------------------------

def download_file(url, destination):

    logger.info("Downloading: %s", url)

    with session.get(
        url,
        stream=True,
        timeout=(30, 300)
    ) as response:

        response.raise_for_status()

        with open(destination, "wb") as output:

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):

                if chunk:
                    output.write(chunk)

    logger.info("Download completed.")


# --------------------------------------------------
# Decompress and Upload
# --------------------------------------------------

def process_lacnic_file(client):

    compressed_filename = "lacnic.db.gz"
    decompressed_filename = "lacnic.db"

    logger.info("Starting LACNIC database processing.")

    with tempfile.TemporaryDirectory(prefix="lacnic_") as temp_dir:

        compressed_path = Path(temp_dir) / compressed_filename
        decompressed_path = Path(temp_dir) / decompressed_filename

        # Step 1: Download compressed database temporarily
        download_file(LACNIC_URL, compressed_path)

        # Step 2: Decompress the .gz file
        logger.info("Decompressing: %s", compressed_filename)

        with gzip.open(compressed_path, "rb") as source:

            with open(decompressed_path, "wb") as output:

                while True:

                    chunk = source.read(1024 * 1024)

                    if not chunk:
                        break

                    output.write(chunk)

        logger.info("Decompression completed.")

        # Step 3: Upload ONLY decompressed file to MinIO
        client.fput_object(
            BUCKET_NAME,
            decompressed_filename,
            str(decompressed_path),
            content_type="application/octet-stream"
        )

        logger.info(
            "Uploaded decompressed file: %s",
            decompressed_filename
        )

    # Temporary .gz and decompressed files are deleted here
    logger.info("LACNIC database processing completed.")


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    client = create_minio_client()

    ensure_bucket_exists(client)

    try:

        process_lacnic_file(client)

        logger.info(
            "LACNIC database processing completed successfully."
        )

    except Exception:

        logger.exception(
            "Failed to process the LACNIC database."
        )

        raise


# --------------------------------------------------
# Entry Point
# --------------------------------------------------

if __name__ == "__main__":
    main()
