
import os
import gzip
import logging
import tempfile
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from pathlib import Path

import requests
from minio import Minio


# --------------------------------------------------
# Configuration
# --------------------------------------------------

AFRINIC_URL = "https://ftp.afrinic.net/dbase/"
BUCKET_NAME = "afrinic"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
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
    "User-Agent": "AFRINIC-Minio-Downloader/1.0"
})


# --------------------------------------------------
# Parse HTML directory listing
# --------------------------------------------------

class LinkParser(HTMLParser):

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            attributes = dict(attrs)
            href = attributes.get("href")

            if href:
                self.links.append(href)


# --------------------------------------------------
# Discover .gz files
# --------------------------------------------------

def get_gz_files():
    """Discover .gz files in the AFRINIC directory."""

    logger.info("Reading AFRINIC directory: %s", AFRINIC_URL)

    response = session.get(AFRINIC_URL, timeout=60)
    response.raise_for_status()

    parser = LinkParser()
    parser.feed(response.text)

    base = urlparse(AFRINIC_URL)
    gz_files = []

    for link in parser.links:

        file_url = urljoin(AFRINIC_URL, link)
        parsed = urlparse(file_url)

        filename = os.path.basename(parsed.path)

        # Accept only .gz files
        if not filename.lower().endswith(".gz"):
            continue

        # Only accept files from the same host
        if parsed.scheme not in ("http", "https"):
            continue

        if parsed.netloc != base.netloc:
            continue

        # Only accept files directly inside the AFRINIC directory
        if parsed.path.rsplit("/", 1)[0] != base.path.rstrip("/"):
            continue

        gz_files.append(file_url)

    gz_files = sorted(set(gz_files))

    logger.info("Found %d .gz files.", len(gz_files))

    return gz_files


# --------------------------------------------------
# MinIO Setup
# --------------------------------------------------

def create_minio_client():

    if not MINIO_ENDPOINT:
        raise ValueError("MINIO_ENDPOINT environment variable is missing.")

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

    logger.info("Downloading: %s", os.path.basename(destination))

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


# --------------------------------------------------
# Decompress and Upload
# --------------------------------------------------

def process_file(client, file_url):

    filename = os.path.basename(urlparse(file_url).path)

    decompressed_filename = filename[:-3]

    logger.info("Processing: %s", filename)

    with tempfile.TemporaryDirectory(prefix="afrinic_") as temp_dir:

        compressed_path = Path(temp_dir) / filename

        decompressed_path = (
            Path(temp_dir) / decompressed_filename
        )

        # Step 1: Download .gz file temporarily
        download_file(file_url, compressed_path)

        # Step 2: Decompress the file
        logger.info("Decompressing: %s", filename)

        with gzip.open(compressed_path, "rb") as source:

            with open(decompressed_path, "wb") as output:

                while True:

                    chunk = source.read(1024 * 1024)

                    if not chunk:
                        break

                    output.write(chunk)

        # Step 3: Upload ONLY decompressed file
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

    # Temporary files are automatically deleted
    logger.info("Completed: %s", filename)


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    client = create_minio_client()

    ensure_bucket_exists(client)

    files = get_gz_files()

    if not files:
        logger.warning("No .gz files found.")
        return

    successful = 0
    failed = 0

    failures = []

    logger.info("Starting AFRINIC processing...")

    for index, file_url in enumerate(files, start=1):

        logger.info(
            "Processing file %d/%d",
            index,
            len(files)
        )

        try:

            process_file(client, file_url)
            successful += 1

        except Exception as exc:

            failed += 1
            failures.append(file_url)

            logger.exception(
                "Failed to process %s: %s",
                file_url,
                exc
            )

    # Final summary
    logger.info("=" * 50)
    logger.info("AFRINIC processing completed.")
    logger.info("Total files: %d", len(files))
    logger.info("Successful: %d", successful)
    logger.info("Failed: %d", failed)
    logger.info("=" * 50)

    if failures:
        logger.error("Failed files:")

        for failed_file in failures:
            logger.error("%s", failed_file)


# --------------------------------------------------
# Entry Point
# --------------------------------------------------

if __name__ == "__main__":
    main()
