from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core import retry
import io
import datetime
import logging
import os
from typing import List, Optional, Dict
from pathlib import Path
from airflow.exceptions import AirflowException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
FILES_TO_FETCH = [
    "aws_data_desafio.csv",
    "lista_precios.json"
]

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

class DriveToGCSIngestionError(Exception):
    """Custom exception for ingestion errors"""
    pass

def initialize_clients(credentials_path: str) -> Dict:
    """
    Initialize Google Drive and Storage clients.
    
    Args:
        credentials_path: Path to the service account credentials file
        
    Returns:
        Dict: Dictionary containing initialized clients
        
    Raises:
        AirflowException: If client initialization fails
    """
    try:
        creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=creds)
        storage_client = storage.Client()
        
        return {
            'drive': drive_service,
            'storage': storage_client
        }
    except Exception as e:
        error_msg = f"Failed to initialize clients: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

@retry.Retry(predicate=retry.if_exception_type(Exception))
def find_file_id_by_name(drive_service, file_name: str) -> str:
    """
    Find a file ID in Google Drive by its name.
    
    Args:
        drive_service: Google Drive service instance
        file_name: Name of the file to find
        
    Returns:
        str: File ID
        
    Raises:
        DriveToGCSIngestionError: If file is not found
    """
    try:
        query = f"name='{file_name}' and trashed=false"
        results = drive_service.files().list(q=query, pageSize=1, fields="files(id, name)").execute()
        files = results.get('files', [])
        if not files:
            raise DriveToGCSIngestionError(f"File not found in Drive: {file_name}")
        return files[0]['id']
    except Exception as e:
        logger.error(f"Error finding file {file_name}: {str(e)}")
        raise

@retry.Retry(predicate=retry.if_exception_type(Exception))
def download_and_upload(drive_service, storage_client, file_name: str, bucket_name: str) -> None:
    """
    Download a file from Drive and upload it to GCS.
    
    Args:
        drive_service: Google Drive service instance
        storage_client: Google Cloud Storage client
        file_name: Name of the file to process
        bucket_name: Name of the GCS bucket to upload to
        
    Raises:
        DriveToGCSIngestionError: If download or upload fails
    """
    try:
        file_id = find_file_id_by_name(drive_service, file_name)
        request = drive_service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.info(f"Downloading {file_name}: {int(status.progress() * 100)}%")

        buffer.seek(0)

        # Partitioned date format: YYYY/MM/DD
        today = datetime.datetime.now().strftime("%Y/%m/%d")
        base_name = os.path.splitext(file_name)[0]
        destination_path = f"{base_name}/{today}/{file_name}"
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_path)
        
        blob.upload_from_file(buffer, rewind=True)
        logger.info(f"Uploaded to GCS: gs://{bucket_name}/{destination_path}")
        
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {str(e)}")
        raise DriveToGCSIngestionError(f"Failed to process {file_name}: {str(e)}")

def run_ingestion(credentials_path: str, raw_bucket: str, **context) -> None:
    """
    Run the ingestion process for all configured files.
    
    Args:
        credentials_path: Path to the service account credentials file
        raw_bucket: Name of the GCS bucket to upload to
        context: Airflow context dictionary containing execution context
        
    Raises:
        AirflowException: If ingestion fails
    """
    try:
        # Initialize clients
        clients = initialize_clients(credentials_path)
        
        # Process each file
        for file_name in FILES_TO_FETCH:
            try:
                logger.info(f"Starting ingestion for {file_name}")
                download_and_upload(
                    drive_service=clients['drive'],
                    storage_client=clients['storage'],
                    file_name=file_name,
                    bucket_name=raw_bucket
                )
                logger.info(f"Successfully completed ingestion for {file_name}")
            except DriveToGCSIngestionError as e:
                error_msg = f"Failed to ingest {file_name}: {str(e)}"
                logger.error(error_msg)
                raise AirflowException(error_msg)
            except Exception as e:
                error_msg = f"Unexpected error processing {file_name}: {str(e)}"
                logger.error(error_msg)
                raise AirflowException(error_msg)
                
    except Exception as e:
        error_msg = f"Failed to run ingestion process: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

if __name__ == "__main__":
    # For local testing, use environment variables
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')
    raw_bucket = os.getenv('RAW_BUCKET', 'melithirdparty-raw')
    run_ingestion(credentials_path=credentials_path, raw_bucket=raw_bucket)
