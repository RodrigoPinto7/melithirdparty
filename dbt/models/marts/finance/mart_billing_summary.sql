WITH joined_data AS (
  SELECT
    b.start_date,
    DATE_TRUNC(b.start_date, MONTH) AS month,
    b.service_code,
    b.product_code,
    b.usage_amount,
    b.pricing_unit,
    b.net_cost,
    p.precio_lista,
    b.usage_amount * IFNULL(p.precio_lista, 0) AS gross_cost
  FROM {{ ref('stg_aws_billing') }} b
  LEFT JOIN {{ ref('stg_aws_prices') }} p
    ON b.product_code = p.product_code AND b.pricing_unit = p.pricing_unit
),

top5_services AS (
  SELECT
    service_code,
    SUM(usage_amount * IFNULL(precio_lista, 0)) AS gross_cost
  FROM joined_data
  GROUP BY service_code
  ORDER BY gross_cost DESC
  LIMIT 5
),

billing_with_discounts AS (
  SELECT
    jd.*,
    CASE
      WHEN jd.service_code IN (SELECT service_code FROM top5_services)
      THEN 0.7 * jd.gross_cost
      ELSE jd.gross_cost
    END AS discounted_cost
  FROM joined_data jd
),

monthly_summary AS (
  SELECT
    month,
    SUM(discounted_cost) AS total_cost,
    SUM(net_cost) AS net_cost,
    SUM(CASE WHEN discounted_cost < net_cost THEN net_cost - discounted_cost ELSE 0 END) AS potential_refund,
    CASE
      WHEN SUM(net_cost) > 50000 THEN 0.8 * SUM(discounted_cost)
      ELSE SUM(discounted_cost)
    END AS final_billed_cost
  FROM billing_with_discounts
  GROUP BY month
)

SELECT * FROM monthly_summary
