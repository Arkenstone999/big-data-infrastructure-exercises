import gzip
import io
import json
import os
from datetime import datetime, timedelta

import boto3
import requests
from fastapi import APIRouter, status

from bdi_api.settings import Settings

settings = Settings()
s3 = boto3.client("s3")

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


def download_gzip_and_store_in_s3(url, bucket_name, key_name):
    """Downloads a GZIP file and directly uploads it to S3 without saving locally."""
    try:
        response = requests.get(url, stream=True, timeout=24)
        if response.status_code == 200:
            s3.upload_fileobj(response.raw, bucket_name, key_name)
            print(f"Uploaded to S3: {bucket_name}/{key_name}")
        else:
            print(f"Failed to download {url} (Status: {response.status_code})")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")


@s4.post("/aircraft/download")
def download_data(file_limit: int = 1001) -> str:
    """Downloads aircraft data and uploads it to an S3 bucket."""
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    current_time = datetime.strptime("000000", "%H%M%S")

    for _ in range(file_limit):
        filename = current_time.strftime("%H%M%SZ.json.gz")
        file_url = base_url + filename
        s3_key = f"{s3_prefix_path}{filename}"
        
        download_gzip_and_store_in_s3(file_url, s3_bucket, s3_key)

        current_time += timedelta(seconds=5)
        if current_time.second == 60:
            current_time = current_time.replace(second=0)

    return f"Downloaded {file_limit} files and uploaded them to S3 bucket {s3_bucket}/{s3_prefix_path}"


def clean_folder(folder_path: str) -> None:
    """Clean all files in the specified folder"""
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS S3 and store it in the local `prepared` directory."""
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    local_prepared_path = settings.prepared_dir

    # Clean the prepared directory first
    clean_folder(local_prepared_path)
    os.makedirs(local_prepared_path, exist_ok=True)

    try:
        # List objects in S3 under the given prefix
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)

        if "Contents" not in response:
            return "No files found in S3."

        for obj in response["Contents"]:
            s3_key = obj["Key"]
            filename = os.path.basename(s3_key)
            prepared_file_path = os.path.join(local_prepared_path, filename.replace(".gz", ""))

            # Download and process the file directly from S3
            try:
                # Get the object from S3
                response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
                # Read the compressed content
                with gzip.GzipFile(fileobj=io.BytesIO(response["Body"].read())) as gz_file:
                    data = json.loads(gz_file.read().decode("utf-8"))

                timestamp = data["now"]
                aircraft_data = data["aircraft"]

                processed_data = []
                for record in aircraft_data:
                    processed_data.append(
                        {
                            "icao": record.get("hex", None),
                            "registration": record.get("r", None),
                            "type": record.get("t", None),
                            "lat": record.get("lat", None),
                            "lon": record.get("lon", None),
                            "alt_baro": record.get("alt_baro", None),
                            "timestamp": timestamp,
                            "max_altitude_baro": record.get("alt_baro", None),
                            "max_ground_speed": record.get("gs", None),
                            "had_emergency": record.get("alert", 0) == 1,
                        }
                    )

                # Save processed data locally
                with open(prepared_file_path, "w", encoding="utf-8") as prepared_file:
                    json.dump(processed_data, prepared_file)

                print(f"Processed and saved: {prepared_file_path}")

            except Exception as e:
                print(f"Error processing file {s3_key}: {str(e)}")
                continue

        return f"Prepared data saved to {local_prepared_path}"

    except Exception as e:
        return f"Error accessing S3 or processing data: {str(e)}"