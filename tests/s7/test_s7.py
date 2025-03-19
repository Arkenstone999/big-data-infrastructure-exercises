import pytest
from unittest.mock import MagicMock, patch, ANY
from datetime import datetime
import json
import io
import gzip
from fastapi.testclient import TestClient
from fastapi import FastAPI, HTTPException

from bdi_api.s7.exercise import get_db_connection, prepare_data, list_aircraft, get_aircraft_position, get_aircraft_statistics

app = FastAPI()
client = TestClient(app)

@pytest.fixture
def mock_db_connection():
    """Mock database connection and cursor"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    with patch('bdi_api.s7.exercise.get_db_connection', return_value=mock_conn):
        yield mock_conn, mock_cursor

@pytest.fixture
def mock_s3():
    """Mock S3 client"""
    with patch('bdi_api.s7.exercise.s3') as mock_s3:
        yield mock_s3

# Test database connection
def test_get_db_connection():
    with patch('bdi_api.s7.exercise.psycopg2.connect') as mock_connect:
        with patch('bdi_api.s7.exercise.db_credentials') as mock_credentials:
            mock_credentials.username = 'test_user'
            mock_credentials.password = 'test_pass'
            mock_credentials.host = 'test_host'
            mock_credentials.port = 5432
            mock_credentials.database = 'test_db'
            
            get_db_connection()
            
            mock_connect.assert_called_once_with(
                user='test_user',
                password='test_pass',
                host='test_host',
                port=5432,
                dbname='test_db'
            )

def test_prepare_data_success(mock_s3, mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    
    mock_s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "raw/day=20231101/file1.json.gz"}]
    }
    
    test_data = json.dumps({
        "now": 1698825600,
        "aircraft": [{
            "hex": "a65800",
            "r": "N508DN",
            "t": "A359",
            "lat": 46.577740,
            "lon": -178.413162,
            "alt_baro": 39996,
            "gs": 454,
            "alert": 0
        }]
    }).encode('utf-8')
    
    gz_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz_file:
        gz_file.write(test_data)
    
    mock_body = MagicMock()
    mock_body.read.return_value = gz_buffer.getvalue()
    mock_s3.get_object.return_value = {"Body": mock_body}
    
    result = prepare_data()
    
    mock_s3.list_objects_v2.assert_called_once()
    mock_s3.get_object.assert_called_once()
    
    assert mock_cursor.execute.call_count == 1  # One record in sample data
    mock_conn.commit.assert_called_once()
    
    assert "Processed 1/1 files" in result
    assert "Inserted 1 records" in result

def test_list_aircraft(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    
    mock_cursor.fetchall.return_value = [
        ("a65800", "N508DN", "A359"),
        ("ab5c12", "N831AA", "B789")
    ]
    
    result = list_aircraft(num_results=10, page=0)
    
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()
    
    assert len(result) == 2
    assert result[0]["icao"] == "a65800"
    assert result[0]["registration"] == "N508DN"
    assert result[0]["type"] == "A359"

def test_get_aircraft_position(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    
    test_time = datetime(2025, 3, 17, 11, 29, 45, 945108)
    
    mock_cursor.fetchall.return_value = [
        (test_time, 46.577740, -178.413162, 39996)
    ]
    
    result = get_aircraft_position("a65800", num_results=10, page=0)
    
    mock_cursor.execute.assert_called_once_with(ANY, ("a65800", 10, 0))
    mock_cursor.fetchall.assert_called_once()
    
    assert len(result) == 1
    assert result[0]["lat"] == 46.577740
    assert result[0]["lon"] == -178.413162
    assert result[0]["altitude"] == 39996

def test_get_aircraft_statistics(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    
    mock_cursor.fetchone.return_value = (39996, 454, False)
    
    result = get_aircraft_statistics("a65800")
    
    mock_cursor.execute.assert_called_once_with(ANY, ("a65800",))
    mock_cursor.fetchone.assert_called_once()
    
    assert result["max_altitude_baro"] == 39996
    assert result["max_ground_speed"] == 454
    assert result["had_emergency"] is False
