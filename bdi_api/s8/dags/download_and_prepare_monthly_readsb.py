import gzip
import json
import logging
from datetime import timedelta
from pathlib import Path
from urllib.parse import urljoin

import pendulum
import psycopg2.extras
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Constants
DATE_MIN = pendulum.datetime(2023, 11, 1, tz="UTC")
DATE_MAX = pendulum.datetime(2024, 11, 1, tz="UTC")
RAW_BASE = "/tmp/readsb/raw"
PREP_BASE = "/tmp/readsb/prepared"
BASE_URL = "https://samples.adsbexchange.com/readsb-hist"
AWS_CONN_ID = "aws_default"
PG_CONN_ID = "postgres_default"

# Configuration via Airflow Variables
BUCKET = Variable.get("aircraft_s3_bucket", default_var="bdi-aircraft-charles")
FILE_LIMIT = int(Variable.get("readsb_file_limit", default_var=100))

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

# Helpers
def get_first_of_month(dt):
    return dt.replace(day=1)

def valid_date(dt):
    first = get_first_of_month(dt)
    return first.day == 1 and DATE_MIN <= first <= DATE_MAX

def safe_int_alt(alt):
    if isinstance(alt, (int, float)):
        return int(alt)
    if alt == "ground":
        return 0
    return None

# Tasks
def create_dirs():
    Path(RAW_BASE).mkdir(parents=True, exist_ok=True)
    Path(PREP_BASE).mkdir(parents=True, exist_ok=True)
    logging.info("Local temp directories ready")


def download_data(ds, **kwargs):
    exec_dt = pendulum.parse(ds)
    if not valid_date(exec_dt):
        raise AirflowSkipException("Date not in valid range for this DAG")

    y = exec_dt.format("YYYY")
    m = exec_dt.format("MM")
    d = "01"
    date_str = f"{y}{m}{d}"

    raw_dir = Path(RAW_BASE) / f"day={date_str}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    downloaded = 0
    current = pendulum.parse("00:00:00", strict=False)

    for _ in range(FILE_LIMIT + 5):
        if downloaded >= FILE_LIMIT:
            break

        fname = current.format("HHmmss") + "Z.json.gz"
        s3_key = f"raw/readsb/day={date_str}/{fname}"

        if s3.check_for_key(key=s3_key, bucket_name=BUCKET):
            logging.info(f"Skipping existing in S3: {s3_key}")
        else:
            url = urljoin(f"{BASE_URL}/{y}/{m}/{d}/", fname)
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                local_file = raw_dir / fname
                with open(local_file, "wb") as f:
                    f.write(resp.content)
                s3.load_file(
                    filename=str(local_file),
                    key=s3_key,
                    bucket_name=BUCKET,
                    replace=True
                )
                downloaded += 1
                logging.info(f"Downloaded & uploaded: {fname}")
            except Exception as e:
                logging.warning(f"Failed to retrieve {url}: {e}")

        current = current.add(seconds=5)

    logging.info(f"Total raw downloaded: {downloaded}")
    return {"downloaded": downloaded, "date": date_str}


def prepare_data(ds, **kwargs):
    exec_dt = pendulum.parse(ds)
    if not valid_date(exec_dt):
        raise AirflowSkipException("Date not in valid range for this DAG")

    y = exec_dt.format("YYYY")
    m = exec_dt.format("MM")
    d = "01"
    date_str = f"{y}{m}{d}"

    raw_dir = Path(RAW_BASE) / f"day={date_str}"
    prep_dir = Path(PREP_BASE) / f"day={date_str}"
    prep_dir.mkdir(parents=True, exist_ok=True)

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # Ensure table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS aircraft_data (
        id SERIAL PRIMARY KEY,
        icao VARCHAR(10),
        registration VARCHAR(20),
        type VARCHAR(20),
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        alt_baro INTEGER,
        timestamp BIGINT,
        max_altitude_baro INTEGER,
        max_ground_speed DOUBLE PRECISION,
        had_emergency BOOLEAN,
        file_date VARCHAR(8),
        file_name VARCHAR(50),
        UNIQUE(icao, timestamp, file_name)
    );
    """)
    conn.commit()

    insert_sql = """
    INSERT INTO aircraft_data (
        icao, registration, type, lat, lon,
        alt_baro, timestamp, max_altitude_baro,
        max_ground_speed, had_emergency,
        file_date, file_name
    ) VALUES %s
    ON CONFLICT (icao, timestamp, file_name) DO NOTHING;
    """

    records = []

    for gz in raw_dir.glob("*.json.gz"):
        try:
            # Try gzipped open
            with gzip.open(gz, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, gzip.BadGzipFile):
            # Fallback to plain text
            try:
                with open(gz, encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                logging.error(f"Unable to parse {gz.name}: {e}")
                continue

        ts = data.get("now")
        for ac in data.get("aircraft", []):
            alt = safe_int_alt(ac.get("alt_baro"))
            if alt is None or ac.get("hex") is None:
                continue
            rec = (
                ac.get("hex"),
                ac.get("r", ""),
                ac.get("t", ""),
                ac.get("lat"),
                ac.get("lon"),
                alt,
                ts,
                alt,
                ac.get("gs"),
                ac.get("alert", 0) == 1,
                date_str,
                gz.name
            )
            records.append(rec)

        # Write prepared JSON locally and upload
        prep_file = prep_dir / gz.name.replace(".json.gz", ".json")
        with open(prep_file, "w", encoding="utf-8") as pf:
            json.dump(records[-len(data.get("aircraft", [])):], pf)
        prep_key = f"prepared/readsb/day={date_str}/{prep_file.name}"
        s3.load_file(
            filename=str(prep_file),
            key=prep_key,
            bucket_name=BUCKET,
            replace=True
        )
        logging.info(f"Prepared & uploaded: {prep_file.name}")

    if records:
        psycopg2.extras.execute_values(cur, insert_sql, records)
        conn.commit()
        logging.info(f"Inserted {len(records)} rows into Postgres")

    cur.close()
    conn.close()
    return {"records": len(records), "date": date_str}

# DAG definition
with DAG(
    dag_id="readsb_monthly_s3_postgres",
    default_args=default_args,
    description="Monthly import of readsb-hist into S3 and Postgres",
    schedule_interval="@monthly",
    start_date=DATE_MIN,
    end_date=DATE_MAX,
    catchup=True,
    max_active_runs=1,
    tags=["aircraft", "readsb", "s3", "postgresql"],
) as dag:

    init = PythonOperator(
        task_id="create_dirs",
        python_callable=create_dirs
    )
    download = PythonOperator(
        task_id="download_data",
        python_callable=download_data,
        provide_context=True
    )
    prepare = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
        provide_context=True
    )

    init >> download >> prepare
