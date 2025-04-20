from datetime import datetime
from typing import List, Optional

import psycopg2
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from bdi_api.settings import DBCredentials, Settings

settings       = Settings()
db_credentials = DBCredentials()

s8 = APIRouter(
    prefix="/api/s8",
    tags=["s8"],
    responses={
        status.HTTP_404_NOT_FOUND:          {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid request"},
    },
)

class AircraftReturn(BaseModel):
    icao: str
    registration: Optional[str]
    type: Optional[str]
    owner: Optional[str]        = None  # no data in your tables
    manufacturer: Optional[str]
    model: Optional[str]

class AircraftCO2(BaseModel):
    icao: str
    hours_flown: float
    co2: Optional[float]

def get_conn():
    return psycopg2.connect(
        host=db_credentials.host,
        port=db_credentials.port,
        database=db_credentials.database,
        user=db_credentials.username,
        password=db_credentials.password,
    )

@s8.get("/aircraft/", response_model=List[AircraftReturn])
def list_aircraft(num_results: int = 100, page: int = 0):
    offset = page * num_results
    sql = """
        SELECT DISTINCT d.icao,
               d.registration,
               d.type,
               b.manufacturer,
               b.model
        FROM aircraft_data AS d
        LEFT JOIN aircraft_db   AS b ON d.icao = b.icao
        ORDER BY d.icao
        LIMIT %s OFFSET %s;
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, (num_results, offset))
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        conn.close()

    return [
        AircraftReturn(
            icao=row[0],
            registration=row[1],
            type=row[2],
            manufacturer=row[3],
            model=row[4],
        )
        for row in rows
    ]


@s8.get("/aircraft/{icao}/co2", response_model=AircraftCO2)
def get_aircraft_co2(icao: str, day: str):
    # parse and reformat day → YYYYMMDD
    try:
        dt       = datetime.strptime(day, "%Y-%m-%d")
        date_str = dt.strftime("%Y%m%d")
    except ValueError:
        raise HTTPException(status_code=422, detail="`day` must be YYYY‑MM‑DD")

    # 1) count 5s samples for that icao & day
    count_sql = """
        SELECT COUNT(*)
        FROM aircraft_data
        WHERE icao = %s
          AND file_date = %s;
    """
    # 2) look up galph for this aircraft’s type code
    rate_sql = """
        SELECT f.galph
        FROM (
           SELECT DISTINCT type
           FROM aircraft_data
           WHERE icao = %s
             AND file_date = %s
           LIMIT 1
        ) AS t
        JOIN fuel_rates AS f
          ON f.icao = t.type
        LIMIT 1;
    """

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(count_sql, (icao, date_str))
            rec_count = cur.fetchone()[0]

            cur.execute(rate_sql, (icao, date_str))
            row = cur.fetchone()
            galph = row[0] if row else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        conn.close()

    hours = (rec_count * 5) / 3600.0
    co2   = None
    if galph is not None and hours > 0:
        fuel_gal   = galph * hours
        fuel_kg    = fuel_gal * 3.04
        co2        = (fuel_kg * 3.15) / 907.185

    return AircraftCO2(icao=icao, hours_flown=hours, co2=co2)
