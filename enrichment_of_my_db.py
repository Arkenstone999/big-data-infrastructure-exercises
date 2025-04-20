import json
import psycopg2
import logging
import sys
import os
from datetime import datetime, timedelta

OPS_FILE_PATH = "operators.json"
ACFT_FILE_PATH = "aircrafts.json"
RDS_DB = "aircraft"

#i love logs
log_filename = f"aircraft_data_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_mic_db():
    conn = None
    try:
        logger.info("====[ STEP 1: Loading Environment Variables ]====")
        db_host = os.getenv("BDI_DB_HOST")
        db_port = int(os.getenv("BDI_DB_PORT", 5432))
        db_user = os.getenv("BDI_DB_USERNAME")
        db_pass = os.getenv("BDI_DB_PASSWORD")

        if not all([db_host, db_user, db_pass]):
            raise ValueError("Missing required database environment variables.")

        logger.info("====[ STEP 2: Reading JSON Files ]====")

        # Load operator data
        logger.info(f"Reading {OPS_FILE_PATH}")
        with open(OPS_FILE_PATH, 'r', encoding='utf-8') as f:
            ops_raw = json.load(f)

        operators = [
            {"icao": icao, "operator": details.get('n')}
            for icao, details in ops_raw.items()
            if isinstance(details, dict)
        ]
        logger.info(f"Loaded {len(operators)} operators")

        # Load aircraft data
        logger.info(f"Reading {ACFT_FILE_PATH}")
        with open(ACFT_FILE_PATH, 'r', encoding='utf-8') as f:
            acft_raw = json.load(f)

        infos = []
        for icao, details in acft_raw.items():
            desc = details.get('d') or ''
            if ' ' in desc:
                manufacturer, model = desc.split(' ', 1)
            else:
                manufacturer, model = None, desc
            infos.append({'icao': icao, 'manufacturer': manufacturer, 'model': model})

        logger.info(f"Prepared {len(infos)} aircraft info records")

        logger.info("====[ STEP 3: Connecting to Database ]====")
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=RDS_DB,
            user=db_user,
            password=db_pass
        )
        conn.autocommit = False
        cur = conn.cursor()

        logger.info("Ensuring tables exist")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_operators (
                icao VARCHAR(10) PRIMARY KEY,
                operator TEXT
            );
            CREATE TABLE IF NOT EXISTS aircraft_info (
                icao VARCHAR(10) PRIMARY KEY,
                manufacturer TEXT,
                model TEXT
            );
        """)

        logger.info("Truncating existing data")
        cur.execute("TRUNCATE aircraft_operators, aircraft_info;")

        # Insert operators with progress logs
        logger.info("====[ STEP 4: Inserting Operator Records ]====")
        batch_size = 1000
        start_time = datetime.now()

        for idx, op in enumerate(operators, 1):
            cur.execute(
                "INSERT INTO aircraft_operators (icao, operator) VALUES (%s, %s)",
                (op['icao'], op['operator'])
            )
            if idx % batch_size == 0:
                now = datetime.now()
                elapsed = now - start_time
                logger.info(f"[{now.strftime('%H:%M:%S')}] Inserted {idx}/{len(operators)} operators "
                            f"({(idx / len(operators)) * 100:.1f}%) - Elapsed: {str(elapsed).split('.')[0]}")
        conn.commit()
        logger.info(f"✔ Completed inserting {len(operators)} operator records")

        # Insert aircraft info with progress logs
        logger.info("====[ STEP 5: Inserting Aircraft Info Records ]====")
        start_time = datetime.now()
        for idx, info in enumerate(infos, 1):
            cur.execute(
                "INSERT INTO aircraft_info (icao, manufacturer, model) VALUES (%s, %s, %s)",
                (info['icao'], info['manufacturer'], info['model'])
            )
            if idx % batch_size == 0:
                now = datetime.now()
                elapsed = now - start_time
                logger.info(f"[{now.strftime('%H:%M:%S')}] Inserted {idx}/{len(infos)} aircraft records "
                            f"({(idx / len(infos)) * 100:.1f}%) - Elapsed: {str(elapsed).split('.')[0]}")
        conn.commit()
        logger.info(f"✔ Completed inserting {len(infos)} aircraft info records")

        cur.close()
        logger.info("====[ STEP 6: Done. Closing Connection ]====")

    except Exception as e:
        logger.error(f" Error: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    logger.info("======== AIRCRAFT DATA LOAD SCRIPT STARTED ========")
    start = datetime.now()
    load_mic_db()
    end = datetime.now()
    logger.info(f"✅ Script completed in {str(end - start).split('.')[0]}")
    logger.info("======== AIRCRAFT DATA LOAD SCRIPT FINISHED ========")
