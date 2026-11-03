import os
import gzip
import logging
import tempfile
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
from minio import Minio

# --------------------------------------------------
# Configuration
# --------------------------------------------------

APNIC_URL = "https://ftp.apnic.net/apnic/whois/"
BUCKET_NAME = "apnic"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

session = requests.Session()
session.headers.update({"User-Agent": "RIR-Minio-Downloader/1.0"})

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)


# --------------------------------------------------
# Parse HTML directory listing without BeautifulSoup
# --------------------------------------------------

class LinkParser(HTMLParser):
    """Extract links from an HTML directory listing."""

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
# Check MinIO bucket
# --------------------------------------------------

def ensure_bucket_exists():
    if not minio_client.bucket_exists(BUCKET_NAME):
        raise RuntimeError(
            f"MinIO bucket '{BUCKET_NAME}' does not exist."
        )


# --------------------------------------------------
# Discover all .gz files
# --------------------------------------------------

def get_gz_files():
    logger.info("Reading directory: %s", APNIC_URL)

    response = session.get(APNIC_URL, timeout=60)
    response.raise_for_status()

    parser = LinkParser()
    parser.feed(response.text)

    base = urlparse(APNIC_URL)
    gz_urls = []

    for href in parser.links:
        if not href.lower().endswith(".gz"):
            continue

        file_url = urljoin(APNIC_URL, href)
        parsed = urlparse(file_url)

        # Only accept files directly in the APNIC WHOIS directory.
        if parsed.scheme not in ("http", "https"):
            continue

        if parsed.netloc != base.netloc:
            continue

        if parsed.path.rsplit("/", 1)[0] != base.path.rstrip("/"):
            continue

        gz_urls.append(file_url)

    gz_urls = sorted(set(gz_urls))

    logger.info("Found %d .gz files.", len(gz_urls))
    return gz_urls


# --------------------------------------------------
# Download file
# --------------------------------------------------

def download_file(url, destination):
    with session.get(
        url,
        stream=True,
        timeout=(30, 300),
    ) as response:
        response.raise_for_status()

        with open(destination, "wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)


# --------------------------------------------------
# Upload file to MinIO
# --------------------------------------------------

def upload_file(local_path, object_name, content_type):
    minio_client.fput_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        file_path=local_path,
        content_type=content_type,
    )

    logger.info(
        "Uploaded: %s/%s",
        BUCKET_NAME,
        object_name,
    )


# --------------------------------------------------
# Download, decompress and upload
# --------------------------------------------------

def process_file(url):
    filename = os.path.basename(urlparse(url).path)
    decompressed_filename = filename[:-3]

    logger.info("Processing: %s", filename)

    with tempfile.TemporaryDirectory(prefix="apnic_") as temp_dir:
        gz_path = os.path.join(temp_dir, filename)
        decompressed_path = os.path.join(
            temp_dir,
            decompressed_filename,
        )

        # Step 1: Download the .gz file temporarily.
        download_file(url, gz_path)

        # Step 2: Decompress the downloaded file.
        with gzip.open(gz_path, "rb") as source:
            with open(decompressed_path, "wb") as destination:
                while True:
                    chunk = source.read(1024 * 1024)

                    if not chunk:
                        break

                    destination.write(chunk)

        # Step 3: Upload ONLY the decompressed file to MinIO.
        upload_file(
            decompressed_path,
            decompressed_filename,
            "application/octet-stream",
        )

    logger.info("Completed: %s", decompressed_filename)

# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    ensure_bucket_exists()

    gz_urls = get_gz_files()

    if not gz_urls:
        logger.warning("No .gz files found.")
        return

    failures = []

    for url in gz_urls:
        try:
            process_file(url)
        except Exception:
            logger.exception("Failed to process: %s", url)
            failures.append(url)

    logger.info(
        "Total files: %d | Successful: %d | Failed: %d",
        len(gz_urls),
        len(gz_urls) - len(failures),
        len(failures),
    )

    if failures:
        raise RuntimeError(
            f"Failed to process {len(failures)} files."
        )


if __name__ == "__main__":
    main()
