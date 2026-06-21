-- Stored Procedure: Deduplicate orders → mart_orders
-- Gets the LATEST status per order_id from raw CDC events

CREATE OR REPLACE PROCEDURE `logistics_dwh.sp_deduplicate_orders`()
BEGIN
  -- Clear existing mart data
  DELETE FROM `logistics_dwh.mart_orders` WHERE 1=1;

  -- Insert deduplicated latest state per order
  INSERT INTO `logistics_dwh.mart_orders`
  (order_id, customer_id, order_status, total_amount,
   payment_method, order_date, last_updated, total_updates)
  SELECT
    order_id,
    ANY_VALUE(customer_id)                                    AS customer_id,
    -- Get the status from the most recent CDC event
    ARRAY_AGG(order_status ORDER BY cdc_timestamp DESC)[OFFSET(0)] AS order_status,
    ANY_VALUE(total_amount)                                   AS total_amount,
    ANY_VALUE(payment_method)                                 AS payment_method,
    MIN(CAST(order_date AS TIMESTAMP))                        AS order_date,
    MAX(CAST(cdc_timestamp AS TIMESTAMP))                     AS last_updated,
    COUNT(*)                                                  AS total_updates
  FROM `logistics_dwh.orders`
  WHERE order_id IS NOT NULL
  GROUP BY order_id;

  SELECT CONCAT('✅ mart_orders refreshed: ',
    CAST(COUNT(*) AS STRING), ' orders')                      AS result
  FROM `logistics_dwh.mart_orders`;
END;
