WITH joined_data AS (
  SELECT
    DATE_TRUNC(start_date, MONTH) AS month,
    service_code,
    b.product_code,
    p.pricing_unit,
    usage_amount,
    net_cost,
    p.precio_lista,
    usage_amount * IFNULL(p.precio_lista, 0) AS gross_cost
  FROM {{ ref('stg_aws_billing') }} b
  LEFT JOIN {{ ref('stg_aws_prices') }} p
    ON b.product_code = p.product_code AND b.pricing_unit = p.pricing_unit
),

top5_services AS (
  SELECT service_code
  FROM joined_data
  GROUP BY service_code
  ORDER BY SUM(gross_cost) DESC
  LIMIT 5
),

with_discounts AS (
  SELECT
    jd.*,
    CASE
      WHEN jd.service_code IN (SELECT service_code FROM top5_services)
      THEN 0.7 * jd.gross_cost
      ELSE jd.gross_cost
    END AS discounted_cost
  FROM joined_data jd
),

monthly_agg AS (
  SELECT
    month,
    service_code,
    SUM(usage_amount) AS total_usage,
    SUM(net_cost) AS total_net_cost,
    SUM(discounted_cost) AS total_discounted_cost
  FROM with_discounts
  GROUP BY month, service_code
),

growth_calc AS (
  SELECT
    *,
    LAG(total_discounted_cost) OVER (PARTITION BY service_code ORDER BY month) AS previous_discounted_cost,
    total_discounted_cost - LAG(total_discounted_cost) OVER (PARTITION BY service_code ORDER BY month) AS growth
  FROM monthly_agg
)

SELECT *
FROM growth_calc
