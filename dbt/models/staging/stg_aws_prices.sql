SELECT
  product_code,
  product_name,
  instance_type,
  pricing_term,
  pricing_unit,
  CAST(precio_lista AS FLOAT64) AS precio_lista
FROM `melithirdparty-460619.billing_staging.lista_precios`
WHERE pricing_term = 'OnDemand'
