CREATE OR REPLACE PROCEDURE `logistics_dwh.sp_delivery_kpi`()
BEGIN
  DELETE FROM `logistics_dwh.mart_delivery_kpi` WHERE 1=1;

  INSERT INTO `logistics_dwh.mart_delivery_kpi`
  (report_date, total_orders, delivered_orders, cancelled_orders,
   pending_orders, delivery_rate, avg_order_value,
   top_payment_method, top_courier)

  WITH latest_orders AS (
    SELECT
      order_id,
      ARRAY_AGG(order_status   ORDER BY cdc_timestamp DESC)[OFFSET(0)] AS order_status,
      ARRAY_AGG(total_amount   ORDER BY cdc_timestamp DESC)[OFFSET(0)] AS total_amount,
      ARRAY_AGG(payment_method ORDER BY cdc_timestamp DESC)[OFFSET(0)] AS payment_method
    FROM `logistics_dwh.orders`
    WHERE order_id IS NOT NULL
    GROUP BY order_id
  ),
  payment_counts AS (
    SELECT
      payment_method,
      COUNT(*) AS cnt
    FROM latest_orders
    WHERE payment_method IS NOT NULL
    GROUP BY payment_method
    ORDER BY cnt DESC
    LIMIT 1
  ),
  courier_counts AS (
    SELECT
      courier,
      COUNT(*) AS cnt
    FROM `logistics_dwh.shipments`
    WHERE courier IS NOT NULL
    GROUP BY courier
    ORDER BY cnt DESC
    LIMIT 1
  )

  SELECT
    CURRENT_DATE()                                                AS report_date,
    COUNT(*)                                                      AS total_orders,
    COUNTIF(order_status = 'DELIVERED')                           AS delivered_orders,
    COUNTIF(order_status = 'CANCELLED')                           AS cancelled_orders,
    COUNTIF(order_status IN ('PENDING','PROCESSING'))             AS pending_orders,
    ROUND(SAFE_DIVIDE(
      COUNTIF(order_status = 'DELIVERED'), COUNT(*)
    ) * 100, 2)                                                   AS delivery_rate,
    ROUND(AVG(total_amount), 2)                                   AS avg_order_value,
    ANY_VALUE((SELECT payment_method FROM payment_counts LIMIT 1)) AS top_payment_method,
    ANY_VALUE((SELECT courier FROM courier_counts LIMIT 1))        AS top_courier
  FROM latest_orders;

  SELECT CONCAT('✅ mart_delivery_kpi refreshed: ',
    CAST(COUNT(*) AS STRING), ' rows') AS result
  FROM `logistics_dwh.mart_delivery_kpi`;
END;
