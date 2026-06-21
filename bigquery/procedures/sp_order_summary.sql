-- Stored Procedure: Order items summary per order

CREATE OR REPLACE PROCEDURE `logistics_dwh.sp_order_summary`()
BEGIN
  DELETE FROM `logistics_dwh.mart_order_summary` WHERE 1=1;

  INSERT INTO `logistics_dwh.mart_order_summary`
  (order_id, total_items, total_quantity, total_amount, most_expensive_item)
  SELECT
    order_id,
    COUNT(DISTINCT item_id)   AS total_items,
    SUM(quantity)             AS total_quantity,
    SUM(subtotal)             AS total_amount,
    MAX(unit_price)           AS most_expensive_item
  FROM `logistics_dwh.order_items`
  WHERE order_id IS NOT NULL
  GROUP BY order_id;

  SELECT CONCAT('✅ mart_order_summary refreshed: ',
    CAST(COUNT(*) AS STRING), ' orders')                        AS result
  FROM `logistics_dwh.mart_order_summary`;
END;
