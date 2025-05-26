from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta

# Configuración
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
        "schema": []  # autodetectar
    }
]

storage_client = storage.Client()
bq_client = bigquery.Client(project=PROJECT_ID)
raw_bucket = storage_client.bucket(RAW_BUCKET_NAME)
stage_bucket = storage_client.bucket(STAGE_BUCKET_NAME)
today = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")

for file in FILES:
    file_name = file["name"]
    base_name = file_name.split(".")[0]
    source_path = f"{base_name}/{today}/{file_name}"
    parquet_path = f"{base_name}/{today}/{base_name}.parquet"

    # Descargar desde bucket RAW
    blob = raw_bucket.blob(source_path)
    data = blob.download_as_bytes()

    # Convertir a DataFrame
    if file_name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(data))
        # Parsear fecha si está presente
        if "start_date" in df.columns:
            # Parse date string with explicit format
            df["start_date"] = pd.to_datetime(df["start_date"], format='%Y-%m-%d', errors='coerce')

    elif file_name.endswith(".json"):
        df = pd.read_json(io.BytesIO(data))
    else:
        raise ValueError("Formato de archivo no soportado")

    # Guardar como Parquet en memoria con compresión Snappy
    table = pa.Table.from_pandas(df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer, compression="snappy")
    parquet_buffer.seek(0)

    # Subir Parquet al bucket STAGE
    stage_blob = stage_bucket.blob(parquet_path)
    stage_blob.upload_from_file(parquet_buffer, rewind=True)
    print(f"Archivo Parquet subido: gs://{STAGE_BUCKET_NAME}/{parquet_path}")

    # Cargar en BigQuery con partición por start_date si aplica
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{base_name}"
    uri = f"gs://{STAGE_BUCKET_NAME}/{parquet_path}"

    # Check if table exists and delete if it does to apply partitioning
    try:
        bq_client.get_table(table_id)
        print(f"Deleting existing table: {table_id}")
        bq_client.delete_table(table_id)
    except Exception as e:
        print(f"Table {table_id} does not exist or could not be deleted: {e}")

    # Define table schema and partitioning
    table = bigquery.Table(table_id, schema=file["schema"])
    if file_name == "aws_data_desafio.csv":
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="start_date"
        )

    # Create the table
    print(f"Creating table: {table_id} with partitioning")
    bq_client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=file["schema"] if file["schema"] else None,
        autodetect=not file["schema"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="start_date"
        ) if file_name == "aws_data_desafio.csv" else None
    )

    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    print(f"Carga completa a BigQuery: {table_id} (particionado por 'start_date' si aplica)")
