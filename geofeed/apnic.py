from __future__ import annotations
import ipaddress
import io
import logging
import os
import re
from datetime import date
from typing import Dict, Iterator, List, Optional, Tuple
import tempfile

import psycopg2
from minio import Minio

RIR_NAME = "APNIC"
RIR_FULLNAME = "Asia Pacific Network Information Centre"
MINIO_BUCKET = "apnic"
MINIO_OBJECTS = [('apnic.db.inetnum', 4), ('apnic.db.inet6num', 6)]

LOG = logging.getLogger("geofeed.apnic")
URL_RE = re.compile(r"https?://[^\s,;<>\"']+", re.IGNORECASE)


def _minio_client() -> Minio:
    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes"),
        region=os.getenv("MINIO_REGION") or None,
    )


def _postgres_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "geofeed_db"),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "15")),
        sslmode=os.getenv("PGSSLMODE", "disable"),
        application_name="geofeed_airflow",
    )

def _ensure_schema(conn) -> None:
    # The advisory lock serializes DDL when the four Airflow tasks start together.
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(782341, 20261104)")
        cur.execute("""
            ALTER TABLE public.geofeed_ipv4
                ADD COLUMN IF NOT EXISTS first_ip INET,
                ADD COLUMN IF NOT EXISTS last_ip INET
        """)
        cur.execute("""
            ALTER TABLE public.geofeed_ipv6
                ADD COLUMN IF NOT EXISTS first_ip INET,
                ADD COLUMN IF NOT EXISTS last_ip INET
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_rir_master_rir_name
                ON public.rir_master (rir_name)
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_geofeed_files_rir_url
                ON public.geofeed_files (rir_id, geofeed_files)
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_geofeed_ipv4_ingest
                ON public.geofeed_ipv4
                   (rir_id, geofeed_file_id, inetnum, snapshot_date, source)
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_geofeed_ipv6_ingest
                ON public.geofeed_ipv6
                   (rir_id, geofeed_file_id, inet6num, snapshot_date, source)
        """)
    conn.commit()


def _get_rir_id(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.rir_master (rir_name, rir_fullname)
            VALUES (%s, %s)
            ON CONFLICT (rir_name)
            DO UPDATE SET rir_fullname = EXCLUDED.rir_fullname
            RETURNING rir_id
            """,
            (RIR_NAME, RIR_FULLNAME),
        )
        rir_id = cur.fetchone()[0]
    conn.commit()
    return rir_id


def _get_geofeed_file_id(conn, rir_id: int, url: str, cache: Dict[str, int]) -> int:
    if url in cache:
        return cache[url]

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.geofeed_files (rir_id, geofeed_files)
            VALUES (%s, %s)
            ON CONFLICT (rir_id, geofeed_files) DO NOTHING
            """,
            (rir_id, url),
        )
        cur.execute(
            """
            SELECT geofeed_file_id
            FROM public.geofeed_files
            WHERE rir_id = %s AND geofeed_files = %s
            """,
            (rir_id, url),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Unable to resolve geofeed_file_id for {url}")
        cache[url] = row[0]
        return row[0]


def _iter_rpsl_records(text_stream) -> Iterator[dict]:
    """Yield RPSL records separated by blank lines; append indented continuations."""
    record = {}
    last_key = None

    for raw_line in text_stream:
        line = raw_line.rstrip("\r\n")

        if not line.strip():
            if record:
                yield record
                record = {}
                last_key = None
            continue

        if line.lstrip().startswith(("%", "#")):
            continue

        if line[:1].isspace():
            if last_key and record.get(last_key):
                continuation = line.strip()
                if continuation:
                    record[last_key][-1] += " " + continuation
            continue

        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip().lower()
        if key:
            record.setdefault(key, []).append(value.strip())
            last_key = key

    if record:
        yield record


def _extract_geofeed_urls(record: dict) -> List[str]:
    """Extract URLs from geofeed: or remarks: Geofeed URL attributes."""
    urls: List[str] = []

    # Style 1: geofeed: https://example.com/geofeed
    for value in record.get("geofeed", []):
        urls.extend(URL_RE.findall(value))

    # Style 2: remarks: Geofeed https://example.com/geofeed
    # Only accept a URL when "Geofeed" and the URL occur in the same
    # remarks attribute value.
    for value in record.get("remarks", []):
        if re.search(r"\bgeofeed\b", value, flags=re.IGNORECASE):
            urls.extend(URL_RE.findall(value))

    result: List[str] = []
    seen = set()
    for url in urls:
        url = url.rstrip(".,:)]}")
        if url and url not in seen:
            seen.add(url)
            result.append(url)
    return result

def _parse_networks(prefix_value: str, version: int):
    """Parse CIDR, single address, or RPSL start-end range."""
    value = prefix_value.split("#", 1)[0].strip()
    if not value:
        raise ValueError("empty IP prefix")

    if "/" in value:
        networks = [ipaddress.ip_network(value, strict=False)]
    elif "-" in value:
        start_text, end_text = (part.strip() for part in value.split("-", 1))
        start = ipaddress.ip_address(start_text)
        end = ipaddress.ip_address(end_text)
        if start.version != end.version:
            raise ValueError("range contains mixed IP versions")
        if int(start) > int(end):
            raise ValueError("range start is greater than range end")
        networks = list(ipaddress.summarize_address_range(start, end))
    else:
        address = ipaddress.ip_address(value)
        networks = [ipaddress.ip_network(
            f"{address}/{address.max_prefixlen}", strict=False
        )]

    if any(network.version != version for network in networks):
        raise ValueError(f"expected IPv{version} prefix: {prefix_value}")
    return networks


def _record_prefix(record: dict, expected_version: Optional[int]):
    if expected_version == 4:
        values = record.get("inetnum", [])
        return (values[0], 4) if values else (None, None)
    if expected_version == 6:
        values = record.get("inet6num", [])
        return (values[0], 6) if values else (None, None)

    if record.get("inetnum"):
        return record["inetnum"][0], 4
    if record.get("inet6num"):
        return record["inet6num"][0], 6
    return None, None


def _insert_network(
    conn,
    rir_id: int,
    file_id: int,
    network,
    url: str,
    country: Optional[str],
    source_object: str,
    snapshot_date: date,
) -> None:
    first_ip = str(network.network_address)
    last_ip = str(network.broadcast_address)
    country_value = country.strip().upper()[:2] if country else None

    with conn.cursor() as cur:
        if network.version == 4:
            cur.execute(
                """
                INSERT INTO public.geofeed_ipv4
                    (rir_id, geofeed_file_id, inetnum, geofeed_url,
                     country, source, snapshot_date, first_ip, last_ip)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (rir_id, file_id, str(network), url, country_value,
                 source_object, snapshot_date, first_ip, last_ip),
            )
        else:
            cur.execute(
                """
                INSERT INTO public.geofeed_ipv6
                    (rir_id, geofeed_file_id, inet6num, geofeed_url,
                     country, source, snapshot_date, first_ip, last_ip)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (rir_id, file_id, str(network), url, country_value,
                 source_object, snapshot_date, first_ip, last_ip),
            )


def _process_object(
    client,
    conn,
    rir_id: int,
    object_name: str,
    expected_version: Optional[int],
    snapshot_date: date,
) -> dict:
    """Download a MinIO object to temporary disk storage, then parse and ingest it."""
    stats = {
        "object": object_name,
        "bytes_downloaded": 0,
        "records_seen": 0,
        "records_with_geofeed": 0,
        "urls_found": 0,
        "networks_parsed": 0,
        "prefix_rows_attempted": 0,
        "invalid_prefixes": 0,
        "database_commits": 0,
    }
    file_id_cache: Dict[str, int] = {}
    pending = 0
    response = None
    temp_file = None
    text_stream = None

    LOG.info("=" * 72)
    LOG.info("Starting object processing: %s/%s", MINIO_BUCKET, object_name)
    LOG.info("Expected IP version: %s", expected_version or "IPv4/IPv6")
    LOG.info("Snapshot date: %s", snapshot_date)

    try:
        # Download to a disk-backed file. Do not wrap MinIO's HTTP response
        # directly in TextIOWrapper; its lifecycle can close during iteration.
        response = client.get_object(MINIO_BUCKET, object_name)
        temp_file = tempfile.TemporaryFile(mode="w+b")
        chunk_size = 1024 * 1024
        next_progress_log = 100 * 1024 * 1024

        for chunk in response.stream(amt=chunk_size, decode_content=True):
            if not chunk:
                continue
            temp_file.write(chunk)
            stats["bytes_downloaded"] += len(chunk)
            if stats["bytes_downloaded"] >= next_progress_log:
                LOG.info(
                    "Download progress for %s: %.2f MB",
                    object_name,
                    stats["bytes_downloaded"] / (1024 * 1024),
                )
                next_progress_log += 100 * 1024 * 1024

        temp_file.flush()
        temp_file.seek(0)
        LOG.info(
            "Download completed for %s: %.2f MB",
            object_name,
            stats["bytes_downloaded"] / (1024 * 1024),
        )

        response.close()
        response.release_conn()
        response = None
        LOG.info("MinIO response closed and connection released.")

        text_stream = io.TextIOWrapper(
            temp_file, encoding="utf-8", errors="replace"
        )
        LOG.info("Starting RPSL parsing for %s", object_name)

        for record in _iter_rpsl_records(text_stream):
            stats["records_seen"] += 1

            if stats["records_seen"] % 100000 == 0:
                LOG.info(
                    "Parsing progress for %s: records=%d geofeed_records=%d "
                    "rows_attempted=%d",
                    object_name,
                    stats["records_seen"],
                    stats["records_with_geofeed"],
                    stats["prefix_rows_attempted"],
                )

            prefix_value, version = _record_prefix(record, expected_version)
            if not prefix_value or version is None:
                continue

            # Both supported forms are accepted:
            #   geofeed: https://...
            #   remarks: Geofeed https://...
            # The remarks URL must be in the same remarks value.
            urls = _extract_geofeed_urls(record)
            if not urls:
                continue

            stats["records_with_geofeed"] += 1
            stats["urls_found"] += len(urls)

            try:
                networks = _parse_networks(prefix_value, version)
                stats["networks_parsed"] += len(networks)
            except Exception as exc:
                stats["invalid_prefixes"] += 1
                LOG.warning(
                    "Invalid prefix | object=%s record=%d prefix=%r error=%s",
                    object_name, stats["records_seen"], prefix_value, exc,
                )
                continue

            country_values = record.get("country", [])
            country = country_values[0] if country_values else None

            for url in urls:
                file_id = _get_geofeed_file_id(
                    conn, rir_id, url, file_id_cache
                )
                for network in networks:
                    _insert_network(
                        conn, rir_id, file_id, network, url, country,
                        object_name, snapshot_date,
                    )
                    stats["prefix_rows_attempted"] += 1
                    pending += 1

                    if pending >= 500:
                        conn.commit()
                        stats["database_commits"] += 1
                        LOG.info(
                            "Database batch committed: object=%s commits=%d "
                            "rows_attempted=%d",
                            object_name,
                            stats["database_commits"],
                            stats["prefix_rows_attempted"],
                        )
                        pending = 0

        # Commit remaining rows and any file metadata created since last batch.
        conn.commit()
        stats["database_commits"] += 1

        LOG.info("RPSL parsing and database ingestion completed for %s", object_name)
        LOG.info("Object statistics: %s", stats)

        if stats["records_with_geofeed"] == 0:
            LOG.warning(
                "No Geofeed records found in %s. Check source data and RPSL parsing.",
                object_name,
            )

        return stats

    except Exception:
        try:
            conn.rollback()
        except Exception:
            LOG.exception("PostgreSQL rollback failed for %s", object_name)
        LOG.exception("Object processing failed: %s", object_name)
        raise

    finally:
        if text_stream is not None:
            try:
                text_stream.close()  # also closes temp_file
            except Exception:
                LOG.warning("Could not close text stream", exc_info=True)
        elif temp_file is not None:
            try:
                temp_file.close()
            except Exception:
                LOG.warning("Could not close temporary file", exc_info=True)

        if response is not None:
            try:
                response.close()
            except Exception:
                LOG.warning("Could not close MinIO response", exc_info=True)
            try:
                response.release_conn()
            except Exception:
                LOG.warning("Could not release MinIO connection", exc_info=True)

def run() -> None:
    """Airflow callable and standalone entry point."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    LOG.info("Starting %s Geofeed extraction", RIR_NAME)

    client = _minio_client()
    conn = _postgres_connection()

    try:
        _ensure_schema(conn)
        rir_id = _get_rir_id(conn)
        snapshot_date = date.today()

        if not client.bucket_exists(MINIO_BUCKET):
            raise RuntimeError(f"MinIO bucket does not exist: {MINIO_BUCKET}")

        for object_name, version in MINIO_OBJECTS:
            _process_object(
                client, conn, rir_id, object_name, version, snapshot_date
            )

        LOG.info("%s Geofeed extraction completed", RIR_NAME)
    except Exception:
        conn.rollback()
        LOG.exception("%s Geofeed extraction failed", RIR_NAME)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
