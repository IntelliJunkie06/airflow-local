import logging
import os
import subprocess
import sys

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# CONFIGURATION
# ============================================================

SCRIPT_DIR = Path("/opt/airflow/geofeed")
ENV_FILE = SCRIPT_DIR / ".env"

RIR_SCRIPTS = [
    ("apnic", SCRIPT_DIR / "apnic.py"),
    ("afrinic", SCRIPT_DIR / "afrinic.py"),
    ("lacnic", SCRIPT_DIR / "lacnic.py"),
    ("ripe", SCRIPT_DIR / "ripe.py"),
]

logger = logging.getLogger(__name__)


# ============================================================
# LOAD ENVIRONMENT
# ============================================================

def load_configuration():
    """
    Load configuration from rir-minio/.env.

    Variables already provided by the container environment
    take precedence over values in the .env file.
    """

    from dotenv import load_dotenv

    if not ENV_FILE.is_file():
        raise FileNotFoundError(
            f"Configuration file not found: {ENV_FILE}"
        )

    load_dotenv(
        dotenv_path=ENV_FILE,
        override=False,
    )

    required_variables = [
        "MINIO_ENDPOINT",
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "PGHOST",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ]

    missing_variables = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing_variables)
        )

    return {
        "endpoint": os.getenv("MINIO_ENDPOINT"),
        "access_key": os.getenv("MINIO_ROOT_USER"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
        "secure": os.getenv(
            "MINIO_SECURE", "false"
        ).lower() == "true",
    }


# ============================================================
# VALIDATE SCRIPTS
# ============================================================

def validate_scripts():
    """Verify all four RIR scripts exist."""

    for rir_name, script_path in RIR_SCRIPTS:
        if not script_path.is_file():
            raise FileNotFoundError(
                f"{rir_name.upper()} script not found: "
                f"{script_path}"
            )

    logger.info("All four RIR scripts are available.")


# ============================================================
# EXECUTE GEOF EED PIPELINE
# ============================================================

def run_geofeed_pipeline():
    """
    Execute the daily Geofeed extraction pipeline.

    Order:
        1. Load environment configuration.
        2. Validate all RIR scripts.
        3. Execute APNIC.
        4. Execute AFRINIC.
        5. Execute LACNIC.
        6. Execute RIPE.
    """

    config = load_configuration()

    validate_scripts()

    logger.info("Starting daily Geofeed extraction.")

    for rir_name, script_path in RIR_SCRIPTS:

        logger.info(
            "Starting %s Geofeed extraction.",
            rir_name.upper(),
        )

        subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            timeout=4 * 60 * 60,
            env=os.environ.copy(),
            cwd=str(SCRIPT_DIR),
        )

        logger.info(
            "%s Geofeed extraction completed successfully.",
            rir_name.upper(),
        )

    logger.info(
        "Daily Geofeed extraction completed successfully."
    )


# ============================================================
# AIRFLOW DAG
# ============================================================

default_args = {
    "owner": "network-data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="geofeed_daily_extraction",
    description=(
        "Daily Geofeed extraction from APNIC, AFRINIC, "
        "LACNIC, and RIPE databases into PostgreSQL."
    ),
    default_args=default_args,
    start_date=pendulum.datetime(
        2026, 1, 1, tz="Asia/Kolkata"
    ),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["Geofeed", "RIR", "MinIO", "PostgreSQL"],
) as dag:

    extract_geofeed_data = PythonOperator(
        task_id="extract_geofeed_data",
        python_callable=run_geofeed_pipeline,
        execution_timeout=timedelta(hours=5),
    )