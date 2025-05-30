from typing import Dict, List
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)

def create_views(project_id: str, dataset_id: str) -> None:
    """
    Creates or updates BigQuery tables for AWS billing analysis.
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
    """
    client = bigquery.Client()
    
    # First create the billing_with_gross_cost table
    billing_with_gross_cost_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.aws_billing_with_gross_cost` AS
    SELECT
        b.start_date,
        b.product_code,
        b.product_name,
        b.pricing_term,
        b.pricing_unit,
        b.instance_type,
        b.usage_type,
        b.usage_amount,
        b.net_cost,
        p.precio_lista,
        f.unidad_factor,
        -- Calcular el gross cost ajustando la unidad
        SAFE_DIVIDE(b.usage_amount, f.unidad_factor) * p.precio_lista AS gross_cost,
        -- Indicador de match válido de precio
        p.precio_lista IS NOT NULL AND f.unidad_factor IS NOT NULL AS has_price_match
    FROM `{project_id}.{dataset_id}.stage_aws_billing` b
    LEFT JOIN `{project_id}.{dataset_id}.stage_aws_prices` p
        ON b.product_code = p.product_code
        AND b.product_name = p.product_name
        AND b.pricing_term = p.pricing_term
        AND b.pricing_unit = p.pricing_unit
        AND (b.instance_type = p.instance_type OR (b.instance_type IS NULL AND p.instance_type IS NULL))
    LEFT JOIN `{project_id}.{dataset_id}.stage_aws_unit_factor` f
        ON b.pricing_unit = f.pricing_unit
    """
    
    # Then create the aggregated cost table
    agg_cost_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.aws_agg_cost_by_product_month` AS

    -- 1. Agregamos los datos a nivel producto-mes
    WITH base_agg AS (
        SELECT
            DATE_TRUNC(start_date, MONTH) AS month,
            product_code,
            SUM(usage_amount) AS usage_amount,
            SUM(net_cost) AS net_cost,
            SUM(SAFE_DIVIDE(usage_amount, unidad_factor) * precio_lista) AS gross_cost
        FROM `{project_id}.{dataset_id}.aws_billing_with_gross_cost`
        WHERE has_price_match = TRUE
        GROUP BY month, product_code
    ),

    -- 2. Detectamos top 5 productos por uso (por mes)
    top5_products AS (
        SELECT
            month,
            product_code
        FROM (
            SELECT
                month,
                product_code,
                SUM(usage_amount) AS total_usage,
                RANK() OVER (PARTITION BY month ORDER BY SUM(usage_amount) DESC) AS usage_rank
            FROM base_agg
            GROUP BY month, product_code
        )
        WHERE usage_rank <= 5
    ),

    -- 3. Aplicamos descuentos
    final_cost_calc AS (
        SELECT
            a.*,
            t.product_code IS NOT NULL AS is_top5,
            CASE
                WHEN t.product_code IS NOT NULL AND gross_cost > 50000 THEN 0.56 * gross_cost
                WHEN t.product_code IS NOT NULL THEN 0.7 * gross_cost
                WHEN gross_cost > 50000 THEN 0.8 * gross_cost
                ELSE gross_cost
            END AS expected_cost
        FROM base_agg a
        LEFT JOIN top5_products t
            ON a.month = t.month AND a.product_code = t.product_code
    )

    -- 4. Métricas de auditoría
    SELECT
        *,
        net_cost - expected_cost AS diff_abs,
        SAFE_DIVIDE(net_cost - expected_cost, expected_cost) AS diff_pct,
        ABS(SAFE_DIVIDE(net_cost - expected_cost, expected_cost)) > 0.1 AS needs_review
    FROM final_cost_calc
    """

    # Create the service cost aggregation table
    service_cost_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.aws_agg_service_cost_by_month` AS
    WITH base AS (
        SELECT
            DATE_TRUNC(start_date, MONTH) AS month,
            service_code,
            SUM(net_cost) AS net_cost
        FROM `{project_id}.{dataset_id}.stage_aws_billing`
        WHERE service_code IS NOT NULL
        GROUP BY month, service_code
    ),

    with_growth AS (
        SELECT
            *,
            LAG(net_cost) OVER (PARTITION BY service_code ORDER BY month) AS prev_net_cost
        FROM base
    ),

    final AS (
        SELECT
            *,
            net_cost - prev_net_cost AS growth_abs,
            SAFE_DIVIDE(net_cost - prev_net_cost, prev_net_cost) * 100 AS growth_pct
        FROM with_growth
    )

    SELECT * FROM final
    """
    
    try:
        # Execute the billing_with_gross_cost table creation
        job = client.query(billing_with_gross_cost_query)
        job.result()  # Wait for the job to complete
        logger.info(f"Successfully created/updated table: {project_id}.{dataset_id}.aws_billing_with_gross_cost")
        
        # Execute the aggregated cost table creation
        job = client.query(agg_cost_query)
        job.result()  # Wait for the job to complete
        logger.info(f"Successfully created/updated table: {project_id}.{dataset_id}.aws_agg_cost_by_product_month")

        # Execute the service cost aggregation table creation
        job = client.query(service_cost_query)
        job.result()  # Wait for the job to complete
        logger.info(f"Successfully created/updated table: {project_id}.{dataset_id}.aws_agg_service_cost_by_month")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

def run_view_creation(config: Dict[str, str]) -> None:
    """
    Main function to run the table creation process.
    
    Args:
        config: Dictionary containing project_id and dataset_id
    """
    create_views(
        project_id=config['project_id'],
        dataset_id=config['dataset_id']
    ) 