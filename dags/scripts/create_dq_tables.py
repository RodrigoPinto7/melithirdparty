from typing import Dict
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

def create_dq_tables(project_id: str, dataset_id: str) -> None:
    """
    Creates or updates BigQuery tables for data quality checks.
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
    """
    client = bigquery.Client()
    
    # Create DQ table for stage_aws_billing
    billing_dq_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dq_stage_aws_billing` AS
    SELECT
        'account_id IS NULL' AS check_name, COUNT(*) AS num_issues
    FROM `{project_id}.{dataset_id}.stage_aws_billing`
    WHERE account_id IS NULL
    UNION ALL
    SELECT 'usage_amount < 0', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_billing` WHERE usage_amount < 0
    UNION ALL
    SELECT 'product_code IS NULL', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_billing` WHERE product_code IS NULL
    UNION ALL
    SELECT 'service_code IS NULL', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_billing` WHERE service_code IS NULL
    UNION ALL
    SELECT 'usage_type IS NULL', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_billing` WHERE usage_type IS NULL
    UNION ALL
    SELECT 'instance_type IS NULL', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_billing` WHERE instance_type IS NULL
    UNION ALL
    SELECT 'net_cost IS NULL', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_billing` WHERE net_cost IS NULL
    UNION ALL
    SELECT 'start_date IS NULL', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_billing` WHERE start_date IS NULL
    """
    
    # Create DQ table for stage_aws_prices
    prices_dq_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dq_stage_aws_prices` AS
    SELECT
        'product_code IS NULL' AS check_name, COUNT(*) AS num_issues
    FROM `{project_id}.{dataset_id}.stage_aws_prices`
    WHERE product_code IS NULL
    UNION ALL
    SELECT 'precio_lista IS NULL', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_prices` WHERE precio_lista IS NULL
    UNION ALL
    SELECT 'precio_lista < 0', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_prices` WHERE precio_lista < 0
    UNION ALL
    SELECT 'pricing_unit IS NULL', COUNT(*) FROM `{project_id}.{dataset_id}.stage_aws_prices` WHERE pricing_unit IS NULL
    """
    
    try:
        # Execute the billing DQ table creation
        job = client.query(billing_dq_query)
        job.result()  # Wait for the job to complete
        logger.info(f"Successfully created/updated DQ table: {project_id}.{dataset_id}.dq_stage_aws_billing")
        
        # Execute the prices DQ table creation
        job = client.query(prices_dq_query)
        job.result()  # Wait for the job to complete
        logger.info(f"Successfully created/updated DQ table: {project_id}.{dataset_id}.dq_stage_aws_prices")
    except Exception as e:
        logger.error(f"Error creating DQ tables: {str(e)}")
        raise

def run_dq_creation(config: Dict[str, str]) -> None:
    """
    Main function to run the data quality table creation process.
    
    Args:
        config: Dictionary containing project_id and dataset_id
    """
    create_dq_tables(
        project_id=config['project_id'],
        dataset_id=config['dataset_id']
    ) 