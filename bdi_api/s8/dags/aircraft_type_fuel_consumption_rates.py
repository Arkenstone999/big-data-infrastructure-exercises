import json
import logging
from datetime import timedelta
from pathlib import Path

import psycopg2.extras
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration
FUEL_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
RAW_BASE = "/tmp/fuel/raw"
PREP_BASE = "/tmp/fuel/prepared"
AWS_CONN_ID = "aws_default"
PG_CONN_ID = "postgres_default"
# Fixed S3 bucket
BUCKET = "bdi-aircraft-charles"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_dirs():
    Path(RAW_BASE).mkdir(parents=True, exist_ok=True)
    Path(PREP_BASE).mkdir(parents=True, exist_ok=True)
    logging.info(f"Local dirs ready: {RAW_BASE}, {PREP_BASE}")
    return True

# Download task def
def download_fuel_rates(ds, **kwargs):
    # ds is YYYY-MM-DD
    date_str = ds.replace('-', '')
    raw_dir = Path(RAW_BASE) / f"date={date_str}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    raw_key = f"raw/fuel_rates/date={date_str}/fuel_rates_{date_str}.json"

    # Check idempotency
    if s3.check_for_key(key=raw_key, bucket_name=BUCKET):
        logging.info(f"Raw data already in S3: {raw_key}")
        return {'s3_key': raw_key, 'downloaded': False}

    # Download
    logging.info(f"Downloading fuel rates from {FUEL_URL}")
    resp = requests.get(FUEL_URL, timeout=30)
    resp.raise_for_status()
    fuel_data = json.loads(resp.text)

    local_file = raw_dir / f"fuel_rates_{date_str}.json"
    with open(local_file, 'w', encoding='utf-8') as f:
        json.dump(fuel_data, f)

    # Upload raw to S3
    s3.load_file(
        filename=str(local_file),
        key=raw_key,
        bucket_name=BUCKET,
        replace=True
    )
    logging.info(f"Uploaded raw to S3: {raw_key}")
    return {'s3_key': raw_key, 'downloaded': True}

# Prepare and load task def
def prepare_and_load(ds, **kwargs):
    date_str = ds.replace('-', '')
    raw_key = f"raw/fuel_rates/date={date_str}/fuel_rates_{date_str}.json"
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

    # Read raw JSON directly from S3
    raw_content = s3.read_key(key=raw_key, bucket_name=BUCKET)
    fuel_data = json.loads(raw_content)

    # Prepare records
    records = []
    for icao, meta in fuel_data.items():
        records.append((
            icao,
            meta.get('source'),
            meta.get('name'),
            meta.get('galph'),
            meta.get('category'),
            round(meta.get('galph', 0) * 3.79, 2),
            date_str
        ))

    # Write prepared file locally
    prep_dir = Path(PREP_BASE) / f"date={date_str}"
    prep_dir.mkdir(parents=True, exist_ok=True)
    prep_file = prep_dir / f"fuel_rates_prepared_{date_str}.json"
    with open(prep_file, 'w', encoding='utf-8') as pf:
        json.dump([
            {
                'icao': r[0], 'source': r[1], 'name': r[2], 'galph': r[3],
                'category': r[4], 'kgph': r[5], 'date': r[6]
            }
            for r in records
        ], pf)

    # Upload prepared JSON to S3
    prep_key = f"prepared/fuel_rates/date={date_str}/fuel_rates_prepared_{date_str}.json"
    s3.load_file(
        filename=str(prep_file),
        key=prep_key,
        bucket_name=BUCKET,
        replace=True
    )
    logging.info(f"Uploaded prepared to S3: {prep_key}")

    # Load into PostgreSQL
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fuel_rates (
        icao VARCHAR(10) PRIMARY KEY,
        source TEXT,
        name TEXT,
        galph INTEGER,
        category TEXT,
        kgph REAL,
        date VARCHAR(8)
    );
    """)
    conn.commit()

    insert_sql = """
    INSERT INTO fuel_rates (
        icao, source, name, galph, category, kgph, date
    ) VALUES %s
    ON CONFLICT (icao) DO NOTHING;
    """
    psycopg2.extras.execute_values(cur, insert_sql, records)
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Inserted {len(records)} rows into Postgres fuel_rates")

    return {'inserted': len(records), 'date': date_str}

# DAG definition
from airflow.utils.dates import days_ago

with DAG(
    dag_id='aircraft_fuel_rates_s3_postgres',
    default_args=default_args,
    description='Download, prepare, store fuel rates in S3 and Postgres',
    schedule_interval='0 0 * * 1',  # weekly Monday midnight
    start_date=days_ago(10),
    catchup=False,
    tags=['aircraft', 'fuel', 's3', 'postgresql'],
) as dag:

    init = PythonOperator(
        task_id='create_dirs',
        python_callable=create_dirs
    )

    download = PythonOperator(
        task_id='download_fuel_rates',
        python_callable=download_fuel_rates,
        provide_context=True
    )

    prepare = PythonOperator(
        task_id='prepare_and_load',
        python_callable=prepare_and_load,
        provide_context=True
    )

    init >> download >> prepare
