import gzip
import io
import json
import logging
from datetime import datetime
from typing import Dict, List

import boto3
import psycopg2
from fastapi import APIRouter, HTTPException, status

from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()
TABLE_NAME = "aircraft_data"

# Initialize FastAPI Router
s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

s3 = boto3.client("s3")

# Logger Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Establish a connection to PostgreSQL (AWS RDS) and ensure the table exists."""
    try:
        connection_params = {
            'user': db_credentials.username,
            'password': db_credentials.password,
            'host': db_credentials.host,
            'port': db_credentials.port
        }

        if hasattr(db_credentials, 'database'):
            connection_params['dbname'] = db_credentials.database
        else:
            connection_params['dbname'] = 'postgres'

        logger.info(f"Connecting to database at {connection_params['host']}:{connection_params['port']}")
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # Ensure the required table exists
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            icao VARCHAR(10) NOT NULL,
            registration VARCHAR(20),
            type VARCHAR(20),
            lat NUMERIC(10, 6),
            lon NUMERIC(10, 6),
            alt_baro INTEGER,
            timestamp TIMESTAMP NOT NULL,
            max_altitude_baro INTEGER,
            max_ground_speed INTEGER,
            had_emergency BOOLEAN,
            UNIQUE (icao, timestamp)  -- Prevent duplicate entries
        );
        """)
        conn.commit()
        cursor.close()
        
        return conn

    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection error")

@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Fetch raw aircraft data from S3 and insert into PostgreSQL (RDS)."""
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"  # Adjust for dynamic date-based retrieval
    processed_files = 0
    error_files = 0
    total_records_inserted = 0

    try:
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
        if "Contents" not in response:
            return "No files found in S3."

        total_files = len(response["Contents"])

        for obj in response["Contents"]:
            s3_key = obj["Key"]
            logger.info(f"Processing file: {s3_key}")

            # Create a new connection for each file to isolate transactions
            conn = None
            cursor = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor()

                # Fetch object from S3
                s3_response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
                with gzip.GzipFile(fileobj=io.BytesIO(s3_response["Body"].read())) as gz_file:
                    data = json.loads(gz_file.read().decode("utf-8"))

                # Parse timestamp - ensure it's a proper timestamp
                raw_timestamp = data.get("now")
                if isinstance(raw_timestamp, int):
                    # Convert UNIX timestamp to datetime object
                    timestamp = datetime.fromtimestamp(raw_timestamp)
                else:
                    # Try to parse timestamp string
                    try:
                        timestamp = datetime.fromisoformat(raw_timestamp.replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        timestamp = datetime.now()  # Fallback

                aircraft_data = data.get("aircraft", [])

                inserted = 0
                for record in aircraft_data:
                    # Only process records that have ICAO (required field)
                    icao = record.get("hex")
                    if not icao:
                        continue

                    # Extract fields with proper type handling
                    registration = record.get("r")
                    aircraft_type = record.get("t")
                    lat = record.get("lat")
                    lon = record.get("lon")

                    # Handle altitude - key fix here!
                    alt_baro_raw = record.get("alt_baro")
                    # Convert "ground" to 0, and ensure we have a valid number
                    if alt_baro_raw == "ground" or not isinstance(alt_baro_raw, (int, float)):
                        alt_baro = 0
                    else:
                        alt_baro = alt_baro_raw

                    # Handle ground speed
                    ground_speed_raw = record.get("gs")
                    if not isinstance(ground_speed_raw, (int, float)):
                        ground_speed = 0
                    else:
                        ground_speed = ground_speed_raw

                    had_emergency = bool(record.get("alert", 0) == 1)

                    cursor.execute(
                        f"""
                        INSERT INTO {TABLE_NAME}
                        (icao, registration, type, lat, lon, alt_baro, timestamp,
                         max_altitude_baro, max_ground_speed, had_emergency)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (icao, timestamp) DO NOTHING;
                        """,
                        (
                            icao,
                            registration,
                            aircraft_type,
                            lat,
                            lon,
                            alt_baro,  # Using the fixed value
                            timestamp,
                            alt_baro,  # Using the same fixed value for max_altitude_baro
                            ground_speed,
                            had_emergency,
                        ),
                    )
                    inserted += 1

                # Commit the transaction for this file
                conn.commit()
                processed_files += 1
                total_records_inserted += inserted
                logger.info(f"Successfully processed {inserted} records from {s3_key}")

            except Exception as e:
                error_files += 1
                logger.error(f"Error processing file {s3_key}: {str(e)}")
                # Rollback the transaction for this file
                if conn is not None:
                    conn.rollback()
            finally:
                # Close cursor and connection for this file
                if cursor is not None:
                    cursor.close()
                if conn is not None:
                    conn.close()

        return f"Processed {processed_files}/{total_files} files. Inserted {total_records_inserted} records. Errors: {error_files}"

    except Exception as e:
        logger.error(f"Global error in prepare_data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error accessing S3 or processing files: {str(e)}")

@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> List[Dict]:
    """Lists available aircraft, ordered by ICAO from the PostgreSQL database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT DISTINCT icao, registration, type
            FROM {TABLE_NAME}
            ORDER BY icao
            LIMIT %s OFFSET %s
            """,
            (num_results, page * num_results),
        )

        result = cursor.fetchall()
        cursor.close()
        conn.close()

        return [{"icao": row[0], "registration": row[1], "type": row[2]} for row in result]

    except Exception as e:
        logger.error(f"Error in list_aircraft: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> List[Dict]:
    """Returns all known positions of an aircraft ordered by timestamp from PostgreSQL."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT timestamp, lat, lon, alt_baro
            FROM {TABLE_NAME}
            WHERE icao = %s AND lat IS NOT NULL AND lon IS NOT NULL
            ORDER BY timestamp ASC
            LIMIT %s OFFSET %s
            """,
            (icao, num_results, page * num_results),
        )

        result = cursor.fetchall()
        cursor.close()
        conn.close()

        return [
            {
                "timestamp": row[0].isoformat() if row[0] else None,
                "lat": float(row[1]) if row[1] else None,
                "lon": float(row[2]) if row[2] else None,
                "altitude": row[3]
            }
            for row in result
        ]

    except Exception as e:
        logger.error(f"Error in get_aircraft_position: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> Dict:
    """Returns max altitude, max speed, and emergency status of an aircraft from PostgreSQL."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT
                MAX(max_altitude_baro) as max_alt,
                MAX(max_ground_speed) as max_speed,
                BOOL_OR(had_emergency) as emergency_status
            FROM {TABLE_NAME}
            WHERE icao = %s
            """,
            (icao,),
        )

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail=f"No data found for aircraft with ICAO: {icao}")

        return {
            "max_altitude_baro": result[0] or 0,
            "max_ground_speed": result[1] or 0,
            "had_emergency": result[2] or False,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_aircraft_statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
