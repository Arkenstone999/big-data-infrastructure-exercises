import gzip
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2.extras
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration
DATA_URL = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
RAW_BASE = "/tmp/aircraft/raw"
PREP_BASE = "/tmp/aircraft/prepared"
AWS_CONN_ID = "aws_default"
PG_CONN_ID = "postgres_default"
BUCKET = "bdi-aircraft-charles"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Helpers
def safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None

# Tasks
def create_dirs():
    Path(RAW_BASE).mkdir(parents=True, exist_ok=True)
    Path(PREP_BASE).mkdir(parents=True, exist_ok=True)
    logging.info("Local directories created")


def download_raw(ds, **kwargs):
    date_str = ds.replace("-", "")
    raw_dir = Path(RAW_BASE) / f"date={date_str}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    raw_key = f"raw/basic_ac_db/date={date_str}/basic-ac-db_{date_str}.json.gz"

    if s3.check_for_key(raw_key, BUCKET):
        logging.info(f"Raw file already in S3: {raw_key}")
        return {'raw_key': raw_key, 'downloaded': False}

    resp = requests.get(DATA_URL, timeout=300)
    resp.raise_for_status()
    local_gz = raw_dir / f"basic-ac-db_{date_str}.json.gz"
    with open(local_gz, 'wb') as f:
        f.write(resp.content)

    s3.load_file(
        filename=str(local_gz),
        key=raw_key,
        bucket_name=BUCKET,
        replace=True
    )
    logging.info(f"Uploaded raw to S3: {raw_key}")
    return {'raw_key': raw_key, 'downloaded': True}


def prepare_and_load(ds, **kwargs):
    date_str = ds.replace("-", "")
    raw_key = f"raw/basic_ac_db/date={date_str}/basic-ac-db_{date_str}.json.gz"
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    client = s3.get_conn()
    obj = client.get_object(Bucket=BUCKET, Key=raw_key)
    raw_bytes = obj['Body'].read()
    data_bytes = gzip.decompress(raw_bytes)
    lines = data_bytes.decode('utf-8').splitlines()

    records = []
    prep_dir = Path(PREP_BASE) / f"date={date_str}"
    prep_dir.mkdir(parents=True, exist_ok=True)
    prep_file = prep_dir / f"basic-ac-db_prepared_{date_str}.json"

    with open(prep_file, 'w', encoding='utf-8') as pf:
        for line in lines:
            if not line:
                continue
            obj = json.loads(line)
            pf.write(json.dumps(obj) + '\n')
            records.append((
                obj.get('icao'),
                obj.get('reg'),
                obj.get('manufacturer'),
                obj.get('model'),
                safe_int(obj.get('year')),
                obj.get('mil', False),
                obj.get('faa_pia', False),
                obj.get('faa_ladd', False),
                date_str
            ))

    prep_key = f"prepared/basic_ac_db/date={date_str}/basic-ac-db_prepared_{date_str}.json"
    s3.load_file(
        filename=str(prep_file),
        key=prep_key,
        bucket_name=BUCKET,
        replace=True
    )
    logging.info(f"Uploaded prepared to S3: {prep_key}")

    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircraft_db (
            icao TEXT,
            reg TEXT,
            manufacturer TEXT,
            model TEXT,
            year INT,
            mil BOOLEAN,
            faa_pia BOOLEAN,
            faa_ladd BOOLEAN,
            date VARCHAR(8),
            PRIMARY KEY (icao, date)
        );
    """)
    conn.commit()

    insert_sql = """
        INSERT INTO aircraft_db (
            icao, reg, manufacturer, model, year, mil, faa_pia, faa_ladd, date
        ) VALUES %s
        ON CONFLICT (icao, date) DO NOTHING;
    """
        # Bulk insert in batches for progress logging
    BATCH_SIZE = 10000
    total = len(records)
    logging.info(f"Starting bulk insert of {total} records in batches of {BATCH_SIZE}")
    for idx in range(0, total, BATCH_SIZE):
        batch = records[idx: idx + BATCH_SIZE]
        psycopg2.extras.execute_values(cur, insert_sql, batch)
        conn.commit()
        logging.info(f"Inserted batch {idx // BATCH_SIZE + 1} (records {idx + 1}-{idx + len(batch)})")
    cur.close()
    conn.close()
    logging.info(f"Inserted {len(records)} records into Postgres")
    return {'inserted': len(records)}

# DAG definition
dag = DAG(
    'aircraft_db_s3_postgres',
    default_args=default_args,
    description='Download ADSB aircraft DB to S3 and Postgres',
    schedule_interval='0 0 * * 0',
    start_date=datetime(2023,11,1),
    catchup=False,
)

create = PythonOperator(
    task_id='create_dirs',
    python_callable=create_dirs,
    dag=dag
)

download = PythonOperator(
    task_id='download_raw',
    python_callable=download_raw,
    provide_context=True,
    dag=dag
)

prepare = PythonOperator(
    task_id='prepare_and_load',
    python_callable=prepare_and_load,
    provide_context=True,
    dag=dag
)

create >> download >> prepare


