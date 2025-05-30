from datetime import datetime, timedelta
from typing import Dict, Any
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryExecuteQueryOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup

import logging
import sys
import os
import json
import traceback
from pathlib import Path
from google.cloud import storage

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

# Import custom scripts
from scripts.drive_files_to_gcs import run_ingestion as drive_to_gcs
from scripts.aws_billing_raw_to_stage import run_pipeline
from scripts.create_bigquery_views import run_view_creation
from scripts.create_dq_tables import run_dq_creation

# Constants
DEFAULT_PROJECT_ID = "melithirdparty-460619"
DEFAULT_DATASET_ID = "billing_staging"
DEFAULT_RAW_BUCKET = "melithirdparty-raw"
DEFAULT_STAGE_BUCKET = "melithirdparty-stage"
CREDENTIALS_DIR = Path('/tmp/scripts_creds')

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config() -> Dict[str, str]:
    try:
        config = {
            "project_id": Variable.get("project_id", DEFAULT_PROJECT_ID),
            "dataset_id": Variable.get("dataset_id", DEFAULT_DATASET_ID),
            "raw_bucket": Variable.get("raw_bucket", DEFAULT_RAW_BUCKET),
            "stage_bucket": Variable.get("stage_bucket", DEFAULT_STAGE_BUCKET)
        }
        return config
    except Exception:
        return {
            "project_id": DEFAULT_PROJECT_ID,
            "dataset_id": DEFAULT_DATASET_ID,
            "raw_bucket": DEFAULT_RAW_BUCKET,
            "stage_bucket": DEFAULT_STAGE_BUCKET
        }

def setup_credentials(**context) -> None:
    drive_conn = BaseHook.get_connection('google_drive_credentials')
    CREDENTIALS_DIR.mkdir(exist_ok=True)
    with open(CREDENTIALS_DIR / 'credentials.json', 'w') as f:
        json.dump(json.loads(drive_conn.extra), f)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(CREDENTIALS_DIR / 'credentials.json')

def cleanup_credentials(**context) -> None:
    try:
        if (CREDENTIALS_DIR / 'credentials.json').exists():
            (CREDENTIALS_DIR / 'credentials.json').unlink()
    except Exception as e:
        logger.error(f"Error removing credentials file: {str(e)}")

def check_file_existence(**context) -> None:
    config = get_config()
    client = storage.Client()
    bucket = client.bucket(config['raw_bucket'])
    today = datetime.now().strftime("%Y/%m/%d")
    required_files = [
        f"aws_data_desafio/{today}/aws_data_desafio.csv",
        f"lista_precios/{today}/lista_precios.json"
    ]
    missing = [fp for fp in required_files if not bucket.blob(fp).exists()]
    if missing:
        raise AirflowException(f"Missing files: {', '.join(missing)}")

def create_bigquery_views():
    config = get_config()
    return [
        PythonOperator(
            task_id='create_aws_billing_summary',
            python_callable=run_view_creation,
            op_kwargs={'config': config},
            dag=dag
        )
    ]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'third_party_data_pipeline',
    default_args=default_args,
    description='Pipeline for processing third-party data from Drive to BigQuery',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['third_party', 'data_pipeline'],
)

setup_creds = PythonOperator(
    task_id='setup_credentials',
    python_callable=setup_credentials,
    dag=dag
)

extract_from_drive = PythonOperator(
    task_id='extract_from_drive',
    python_callable=lambda **context: drive_to_gcs(
        credentials_path=str(CREDENTIALS_DIR / 'credentials.json'),
        raw_bucket=get_config()['raw_bucket']
    ),
    dag=dag
)

check_files = PythonOperator(
    task_id='check_files',
    python_callable=check_file_existence,
    dag=dag
)

convert_to_parquet = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=run_pipeline,
    dag=dag
)

config = get_config()

with TaskGroup("data_quality_checks", dag=dag) as dq_checks:
    generate_dq_tables = PythonOperator(
        task_id="generate_dq_tables",
        python_callable=run_dq_creation,
        op_kwargs={'config': config},
        dag=dag
    )

create_views = PythonOperator(
    task_id='create_views',
    python_callable=run_view_creation,
    op_kwargs={'config': config},
    dag=dag
)

cleanup_creds = PythonOperator(
    task_id='cleanup_credentials',
    python_callable=cleanup_credentials,
    trigger_rule='all_done',
    dag=dag
)

setup_creds >> extract_from_drive >> check_files >> convert_to_parquet >> dq_checks >> create_views >> cleanup_creds
