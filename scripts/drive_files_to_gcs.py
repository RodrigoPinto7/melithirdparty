from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.api_core import retry
import io
import datetime
import logging
import os
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Archivos esperados a buscar en Drive por nombre
FILES_TO_FETCH = [
    "aws_data_desafio.csv",
    "lista_precios.json"
]

# Inicialización de clientes
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
CREDS = service_account.Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
DRIVE_SERVICE = build('drive', 'v3', credentials=CREDS)
STORAGE_CLIENT = storage.Client()

BUCKET_NAME = "melithirdparty-raw"

class DriveToGCSIngestionError(Exception):
    """Custom exception for ingestion errors"""
    pass

@retry.Retry(predicate=retry.if_exception_type(Exception))
def find_file_id_by_name(file_name: str) -> str:
    """
    Find a file ID in Google Drive by its name.
    
    Args:
        file_name: Name of the file to find
        
    Returns:
        str: File ID
        
    Raises:
        DriveToGCSIngestionError: If file is not found
    """
    try:
        query = f"name='{file_name}' and trashed=false"
        results = DRIVE_SERVICE.files().list(q=query, pageSize=1, fields="files(id, name)").execute()
        files = results.get('files', [])
        if not files:
            raise DriveToGCSIngestionError(f"Archivo no encontrado en Drive: {file_name}")
        return files[0]['id']
    except Exception as e:
        logger.error(f"Error finding file {file_name}: {str(e)}")
        raise

@retry.Retry(predicate=retry.if_exception_type(Exception))
def download_and_upload(file_name: str) -> None:
    """
    Download a file from Drive and upload it to GCS.
    
    Args:
        file_name: Name of the file to process
        
    Raises:
        DriveToGCSIngestionError: If download or upload fails
    """
    try:
        file_id = find_file_id_by_name(file_name)
        request = DRIVE_SERVICE.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.info(f"Descargando {file_name}: {int(status.progress() * 100)}%")

        buffer.seek(0)

        # Partitioned date format: YYYY/MM/DD
        today = datetime.datetime.now().strftime("%Y/%m/%d")
        base_name = os.path.splitext(file_name)[0]
        destination_path = f"{base_name}/{today}/{file_name}"
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_path)
        
        blob.upload_from_file(buffer, rewind=True)
        logger.info(f"Subido a GCS: gs://{BUCKET_NAME}/{destination_path}")
        
    except Exception as e:
        logger.error(f"Error processing file {file_name}: {str(e)}")
        raise DriveToGCSIngestionError(f"Failed to process {file_name}: {str(e)}")

def run_ingestion() -> None:
    """
    Run the ingestion process for all configured files.
    """
    for file_name in FILES_TO_FETCH:
        try:
            logger.info(f"Starting ingestion for {file_name}")
            download_and_upload(file_name)
            logger.info(f"Successfully completed ingestion for {file_name}")
        except DriveToGCSIngestionError as e:
            logger.error(f"Failed to ingest {file_name}: {str(e)}")
            # Continue with next file even if one fails
            continue
        except Exception as e:
            logger.error(f"Unexpected error processing {file_name}: {str(e)}")
            continue

if __name__ == "__main__":
    run_ingestion()
