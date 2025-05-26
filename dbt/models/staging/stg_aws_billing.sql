SELECT
  CAST(account_id AS STRING) AS account_id,
  CAST(region AS STRING) AS region,
  CAST(product_code AS STRING) AS product_code,
  CAST(service_code AS STRING) AS service_code,
  CAST(product_name AS STRING) AS product_name,
  CAST(usage_type AS STRING) AS usage_type,
  CAST(pricing_unit AS STRING) AS pricing_unit,
  CAST(usage_amount AS FLOAT64) AS usage_amount,
  CAST(net_cost AS FLOAT64) AS net_cost,
  CAST(start_date AS DATE) AS start_date,
  CAST(tag_application AS STRING) AS tag_application
FROM `melithirdparty-460619.billing_staging.aws_data_desafio`
