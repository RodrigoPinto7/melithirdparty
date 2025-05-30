from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from airflow.exceptions import AirflowException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ID = "melithirdparty-460619"
RAW_BUCKET_NAME = "melithirdparty-raw"
STAGE_BUCKET_NAME = "melithirdparty-stage"
DATASET_ID = "billing_staging"

FILES = [
    {
        "name": "aws_data_desafio.csv",
        "schema": [
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("instance_type", "STRING"),
            bigquery.SchemaField("net_cost", "FLOAT"),
            bigquery.SchemaField("pricing_term", "STRING"),
            bigquery.SchemaField("pricing_unit", "STRING"),
            bigquery.SchemaField("product_code", "STRING"),
            bigquery.SchemaField("product_name", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("service_code", "STRING"),
            bigquery.SchemaField("start_date", "DATE"),
            bigquery.SchemaField("tag_application", "STRING"),
            bigquery.SchemaField("usage_amount", "FLOAT"),
            bigquery.SchemaField("usage_type", "STRING")
        ]
    },
    {
        "name": "lista_precios.json",
        "schema": []  # autodetect
    }
]

def initialize_clients() -> Dict:
    """
    Initialize Google Cloud clients.
    
    Returns:
        Dict: Dictionary containing initialized clients
        
    Raises:
        AirflowException: If client initialization fails
    """
    try:
        return {
            'storage': storage.Client(),
            'bigquery': bigquery.Client(project=PROJECT_ID)
        }
    except Exception as e:
        error_msg = f"Failed to initialize clients: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

def read_file_data(storage_client: storage.Client, file_name: str, source_path: str) -> pd.DataFrame:
    """
    Read file data from GCS and convert to DataFrame.
    
    Args:
        storage_client: Google Cloud Storage client
        file_name: Name of the file to read
        source_path: Path to the file in GCS
        
    Returns:
        pd.DataFrame: DataFrame containing the file data
        
    Raises:
        AirflowException: If file reading fails
    """
    try:
        raw_bucket = storage_client.bucket(RAW_BUCKET_NAME)
        blob = raw_bucket.blob(source_path)
        data = blob.download_as_bytes()

        if file_name.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(data))
            if "start_date" in df.columns:
                df["start_date"] = pd.to_datetime(df["start_date"], format='%Y-%m-%d', errors='coerce')
        elif file_name.endswith(".json"):
            df = pd.read_json(io.BytesIO(data))
        else:
            raise ValueError(f"Unsupported file format: {file_name}")

        return df
    except Exception as e:
        error_msg = f"Failed to read file {file_name}: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

def save_as_parquet(df: pd.DataFrame, stage_bucket: storage.Bucket, parquet_path: str) -> None:
    """
    Save DataFrame as Parquet file in GCS.
    
    Args:
        df: DataFrame to save
        stage_bucket: GCS bucket for staging
        parquet_path: Path to save the Parquet file
        
    Raises:
        AirflowException: If saving fails
    """
    try:
        # Define explicit schema for Parquet, especially for date types
        # Convert pandas datetime64[ns] to date32 for BigQuery compatibility
        schema_fields = []
        for col, dtype in df.dtypes.items():
            if col == "start_date":
                schema_fields.append(pa.field("start_date", pa.date32()))
            elif pd.api.types.is_integer_dtype(dtype):
                schema_fields.append(pa.field(col, pa.int64())) # Use int64 for other integers
            elif pd.api.types.is_float_dtype(dtype):
                schema_fields.append(pa.field(col, pa.float64()))
            elif pd.api.types.is_bool_dtype(dtype):
                schema_fields.append(pa.field(col, pa.bool_()))
            else: # Default to string for objects, categories, etc.
                 schema_fields.append(pa.field(col, pa.string()))

        parquet_schema = pa.schema(schema_fields)

        # Convert DataFrame to PyArrow Table with explicit schema
        table = pa.Table.from_pandas(df, schema=parquet_schema, preserve_index=False)

        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer, compression="snappy")
        parquet_buffer.seek(0)

        stage_blob = stage_bucket.blob(parquet_path)
        stage_blob.upload_from_file(parquet_buffer, rewind=True)
        logger.info(f"Parquet file uploaded: gs://{STAGE_BUCKET_NAME}/{parquet_path}")
    except Exception as e:
        error_msg = f"Failed to save Parquet file: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

def load_to_bigquery(
    bq_client: bigquery.Client,
    file_config: Dict[str, Any],
    table_id: str,
    uri: str
) -> None:
    """
    Load data from GCS to BigQuery.
    
    Args:
        bq_client: BigQuery client
        file_config: Configuration for the file
        table_id: Target table ID
        uri: GCS URI of the Parquet file
        
    Raises:
        AirflowException: If loading fails
    """
    try:
        # Delete existing table if it exists
        try:
            bq_client.get_table(table_id)
            logger.info(f"Deleting existing table: {table_id}")
            bq_client.delete_table(table_id)
        except Exception:
            logger.info(f"Table {table_id} does not exist")

        # Create table with schema and partitioning
        table = bigquery.Table(table_id, schema=file_config["schema"])
        if file_config["name"] == "aws_data_desafio.csv":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="start_date"
            )

        logger.info(f"Creating table: {table_id}")
        bq_client.create_table(table)

        # Configure and run load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=file_config["schema"] if file_config["schema"] else None,
            autodetect=not file_config["schema"],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="start_date"
            ) if file_config["name"] == "aws_data_desafio.csv" else None
        )

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        logger.info(f"Data loaded to BigQuery: {table_id}")
    except Exception as e:
        error_msg = f"Failed to load data to BigQuery: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

def process_file(
    clients: Dict,
    file_config: Dict[str, Any],
    today: str
) -> None:
    """
    Process a single file through the pipeline.
    
    Args:
        clients: Dictionary of initialized clients
        file_config: Configuration for the file
        today: Date string in YYYY/MM/DD format
        
    Raises:
        AirflowException: If processing fails
    """
    try:
        file_name = file_config["name"]
        base_name = file_name.split(".")[0]
        source_path = f"{base_name}/{today}/{file_name}"
        parquet_path = f"{base_name}/{today}/{base_name}.parquet"

        # Read and process file
        df = read_file_data(clients['storage'], file_name, source_path)
        
        # Save as Parquet
        stage_bucket = clients['storage'].bucket(STAGE_BUCKET_NAME)
        save_as_parquet(df, stage_bucket, parquet_path)
        
        # Load to BigQuery with new table names
        table_mapping = {
            "aws_data_desafio": "stage_aws_billing",
            "lista_precios": "stage_aws_prices"
        }
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_mapping[base_name]}"
        uri = f"gs://{STAGE_BUCKET_NAME}/{parquet_path}"
        load_to_bigquery(clients['bigquery'], file_config, table_id, uri)
        
    except Exception as e:
        error_msg = f"Failed to process file {file_config['name']}: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

def run_pipeline(**context) -> None:
    """
    Run the complete pipeline for all files.
    
    Args:
        context: Airflow context dictionary containing execution context
        
    Raises:
        AirflowException: If pipeline execution fails
    """
    try:
        # Initialize clients
        clients = initialize_clients()
        
        # Get today's date in YYYY/MM/DD format
        today = datetime.now().strftime("%Y/%m/%d")
        
        # Process each file
        for file_config in FILES:
            process_file(clients, file_config, today)
            
    except Exception as e:
        error_msg = f"Pipeline execution failed: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

if __name__ == "__main__":
    run_pipeline()
