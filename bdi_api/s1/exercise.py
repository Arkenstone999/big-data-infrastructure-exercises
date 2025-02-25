import json
import os
import shutil
from datetime import datetime, timedelta
from typing import Annotated

import requests
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()
RAW_DATA_PATH = os.path.join(settings.raw_dir, "day=20231101")
PREPARED_DATA_PATH = os.path.join(settings.prepared_dir, "day=20231101")

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


def clean_folder(path: str):
    """Cleans the specified folder by removing all its contents."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)


def download_gzip(url, save_path):
    """Downloads a GZIP file and saves it to the specified path."""
    try:
        response = requests.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            with open(save_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded: {url} -> {save_path}")
        else:
            print(f"Failed to download {url} (Status: {response.status_code})")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ..., description="""Limits the number of files to download.""",
        ),
    ] = 1000,
) -> str:
    clean_folder(RAW_DATA_PATH)

    base_url = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
    current_time = datetime.strptime("000000", "%H%M%S")

    for _ in range(file_limit):
        filename = current_time.strftime("%H%M%SZ.json.gz")
        file_url = base_url + filename
        save_path = os.path.join(RAW_DATA_PATH, filename)
        download_gzip(file_url, save_path)

        # Increment by 5 seconds
        current_time += timedelta(seconds=5)
        if current_time.second == 60:
            current_time = current_time.replace(second=0)

    return f"Downloaded {file_limit} files to {RAW_DATA_PATH}"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    clean_folder(PREPARED_DATA_PATH)

    for file_name in os.listdir(RAW_DATA_PATH):
        #print list element
        print(file_name)
        raw_file_path = os.path.join(RAW_DATA_PATH, file_name)
        prepared_file_path = os.path.join(PREPARED_DATA_PATH, file_name.replace(".gz", ""))
        with open(raw_file_path, encoding="utf-8") as raw_file:
            data = json.load(raw_file)
            timestamp = data["now"]
            aircraft_data = data["aircraft"]
            # Example preprocessing: keep only 'icao', 'lat', 'lon', 'alt_baro'
            len(aircraft_data)
            processed_data = []

            for record in aircraft_data:
                processed_data.append({
                    "icao": record.get("hex", None),
                    "registration": record.get("r", None),
                    "type": record.get("t", None),
                    "lat": record.get("lat", None),
                    "lon": record.get("lon", None),
                    "alt_baro": record.get("alt_baro", None),
                    "timestamp": timestamp,
                    "max_altitude_baro" : record.get("alt_baro", None),
                    "max_ground_speed": record.get("gs", None),
                    "had_emergency": record.get("alert", 0) == 1
                })

            with open(prepared_file_path, "w", encoding="utf-8") as prepared_file:
                json.dump(processed_data, prepared_file)
            print("Process finished")
    return f"Prepared data saved to {PREPARED_DATA_PATH}"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    aircraft = set()

    for file_name in os.listdir(PREPARED_DATA_PATH):
        with open(os.path.join(PREPARED_DATA_PATH, file_name), encoding="utf-8") as file:
            data = json.load(file)
            aircraft.update(
                (record["icao"], record.get("registration", "Unknown"), record.get("type", "Unknown"))
                for record in data if record["icao"]
            )

    aircraft_list = sorted(aircraft, key=lambda x: x[0])
    start = page * num_results
    end = start + num_results

    return [
        {"icao": entry[0], "registration": entry[1], "type": entry[2]}
        for entry in aircraft_list[start:end]
    ]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    positions = []

    for file_name in os.listdir(PREPARED_DATA_PATH):
        with open(os.path.join(PREPARED_DATA_PATH, file_name), encoding="utf-8") as file:
            data = json.load(file)
            positions.extend(
                {
                    "timestamp": record.get("timestamp"),
                    "lat": record.get("lat"),
                    "lon": record.get("lon"),
                }
                for record in data if record["icao"] == icao
            )

    positions = sorted(positions, key=lambda x: x["timestamp"])
    start = page * num_results
    end = start + num_results
    return positions[start:end]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    # Initialisation des valeurs par défaut
    alt = 0
    speed = 0
    emergency = False
    data_found = False  # Flag pour vérifier si des données ont été trouvées

    # Parcourir les fichiers de données préparées
    for file_name in os.listdir(PREPARED_DATA_PATH):
        file_path = os.path.join(PREPARED_DATA_PATH, file_name)

        try:
            with open(file_path, encoding="utf-8") as file:
                data = json.load(file)

                for record in data:
                    if record.get("icao", "").lower() == icao.lower():
                        data_found = True

                        # Remplacer None par 0 pour éviter les erreurs
                        record_alt = record.get("max_altitude_baro", 0)
                        record_speed = record.get("max_ground_speed", 0)

                        # Vérifier et remplacer les valeurs None
                        if record_alt is None:
                            record_alt = 0
                        if record_speed is None:
                            record_speed = 0

                        # Mise à jour des valeurs maximales
                        alt = max(alt, record_alt)
                        speed = max(speed, record_speed)

                        # Vérification d'une alerte d'urgence
                        emergency = emergency or record.get("had_emergency", False)

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in file {file_name}: {e}")
        except Exception as e:
            print(f"Unexpected error reading {file_name}: {e}")

    if not data_found:
        return {"error": f"No data found for ICAO {icao}"}

    return {
        "max_altitude_baro": alt,
        "max_ground_speed": speed,
        "had_emergency": emergency,
    }


