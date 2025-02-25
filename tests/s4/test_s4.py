import gzip
import json
import os
from io import BytesIO

import boto3
import pytest
import requests
from moto import mock_s3

from bdi_api.s4.exercise import (
    clean_folder,
    download_data,
    download_gzip_and_store_in_s3,
    prepare_data,
)


# --- Fixture: Mock AWS Credentials ---
@pytest.fixture(autouse=True)
def aws_credentials():
    """Mock AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["BDI_S3_BUCKET"] = "test-bucket"


# --- Fixture: Mock S3 Client ---
@pytest.fixture
def s3_client():
    """Create a mock S3 client using moto."""
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        yield s3


# --- Fixture: Temporary Directory ---
@pytest.fixture
def tmp_dir(tmp_path):
    """Provide a temporary directory for local file operations."""
    os.environ["BDI_LOCAL_DIR"] = str(tmp_path)
    return tmp_path


# --- Fixture: Mock HTTP Requests ---
@pytest.fixture
def mock_requests_get(monkeypatch):
    class MockResponse:
        def __init__(self):
            self.raw = BytesIO(
                gzip.compress(json.dumps({"now": "2023-11-01", "aircraft": [{"hex": "ABC123"}]}).encode("utf-8"))
            )
            self.status_code = 200

        def raise_for_status(self):
            pass

    def mock_get(url, stream=True, timeout=None):
        return MockResponse()

    monkeypatch.setattr(requests, "get", mock_get)


# --- Test: Download GZIP and Store in S3 ---
def test_download_gzip_and_store_in_s3(s3_client, mock_requests_get):
    """Test if a GZIP file is downloaded and uploaded to S3."""
    bucket_name = os.environ["BDI_S3_BUCKET"]
    s3_key = "raw/day=20231101/test.json.gz"

    # Call function (FIX: Removed `s3_client` argument)
    download_gzip_and_store_in_s3("http://example.com/test.gz", bucket_name, s3_key)

    # Verify file exists in S3
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    content = gzip.decompress(response["Body"].read()).decode("utf-8")

    assert json.loads(content) == {"now": "2023-11-01", "aircraft": [{"hex": "ABC123"}]}


# --- Test: Clean Folder ---
def test_clean_folder(tmp_dir):
    """Test if clean_folder removes all files from the given directory."""
    test_file = tmp_dir / "test.txt"
    test_file.write_text("test")

    assert len(list(tmp_dir.glob("*"))) == 1

    clean_folder(str(tmp_dir))

    assert len(list(tmp_dir.glob("*"))) == 0


# --- Test: Download Data ---
def test_download_data(mocker):
    """Test the /aircraft/download endpoint logic."""
    mocker.patch("bdi_api.s4.exercise.download_gzip_and_store_in_s3")

    os.environ["BDI_S3_BUCKET"] = "test-bucket"  # FIX: Ensure correct bucket name

    response = download_data(file_limit=2)

    expected_string = "uploaded them to S3 bucket test-bucket/raw/day=20231101/"
    assert expected_string in response, f"Expected '{expected_string}' but got '{response}'"


# --- Test: Prepare Data ---
def test_prepare_data(s3_client, tmp_dir):
    """Test processing of aircraft data from S3 and saving locally."""
    bucket_name = os.environ["BDI_S3_BUCKET"]
    s3_key = "raw/day=20231101/test.json.gz"
    local_dir = tmp_dir / "prepared"
    os.environ["BDI_LOCAL_DIR"] = str(local_dir)  # FIX: Ensure correct local directory

    # Prepare and upload test JSON data
    sample_data = {
        "now": "2023-11-01T00:00:00",
        "aircraft": [
            {"hex": "ABC123", "lat": 40.7128, "lon": -74.0060, "alt_baro": 1000, "gs": 200, "alert": 1}
        ],
    }

    gz_buffer = BytesIO()
    with gzip.GzipFile(fileobj=gz_buffer, mode="wb") as gz_file:
        gz_file.write(json.dumps(sample_data).encode("utf-8"))

    # Upload to mock S3
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=gz_buffer.getvalue())

    # Run prepare_data
    response = prepare_data()

    # Verify response (FIX: Added debugging info)
    assert response.startswith("Prepared data saved to"), f"Unexpected response: {response}"

    # Verify processed file exists
    prepared_files = list(local_dir.iterdir())
    assert len(prepared_files) == 1, f"Expected 1 file, found {len(prepared_files)}"
    assert prepared_files[0].suffix == ".json"

    # Verify content
    with open(prepared_files[0], encoding="utf-8") as f:
        processed_data = json.load(f)
        assert len(processed_data) == 1
        assert processed_data[0]["icao"] == "ABC123"
        assert processed_data[0]["lat"] == 40.7128
        assert processed_data[0]["lon"] == -74.0060
        assert processed_data[0]["alt_baro"] == 1000
        assert processed_data[0]["max_ground_speed"] == 200
        assert processed_data[0]["had_emergency"] is True
        assert processed_data[0]["timestamp"] == "2023-11-01T00:00:00"


# --- Test: Download GZIP Stream Error Handling ---
def test_download_gzip_and_store_in_s3_error(monkeypatch):
    """Test error handling in download_gzip_and_store_in_s3."""
    def mock_get_error(url, stream=True, timeout=None):
        raise requests.exceptions.RequestException("Network error")

    monkeypatch.setattr(requests, "get", mock_get_error)
    bucket_name = os.environ["BDI_S3_BUCKET"]
    s3_key = "raw/day=20231101/test.json.gz"

    # Call function (FIX: Removed `s3_client` argument)
    download_gzip_and_store_in_s3("http://example.com/test.gz", bucket_name, s3_key)
