-- Stored Procedure: Deduplicate shipments → mart_shipments

CREATE OR REPLACE PROCEDURE `logistics_dwh.sp_deduplicate_shipments`()
BEGIN
  DELETE FROM `logistics_dwh.mart_shipments` WHERE 1=1;

  INSERT INTO `logistics_dwh.mart_shipments`
  (shipment_id, order_id, courier, tracking_number,
   origin_city, destination_city, shipment_status,
   shipped_at, estimated_arrival, total_events, last_updated)
  SELECT
    s.shipment_id,
    ANY_VALUE(s.order_id)                                         AS order_id,
    ANY_VALUE(s.courier)                                          AS courier,
    ANY_VALUE(s.tracking_number)                                  AS tracking_number,
    ANY_VALUE(s.origin_city)                                      AS origin_city,
    ANY_VALUE(s.destination_city)                                 AS destination_city,
    ARRAY_AGG(s.shipment_status ORDER BY s.cdc_timestamp DESC)[OFFSET(0)] AS shipment_status,
    MIN(CAST(s.shipped_at AS TIMESTAMP))                          AS shipped_at,
    MIN(CAST(s.estimated_arrival AS TIMESTAMP))                   AS estimated_arrival,
    COUNT(DISTINCT d.event_id)                                    AS total_events,
    MAX(CAST(s.cdc_timestamp AS TIMESTAMP))                       AS last_updated
  FROM `logistics_dwh.shipments` s
  LEFT JOIN `logistics_dwh.delivery_events` d
    ON d.shipment_id = s.shipment_id
  WHERE s.shipment_id IS NOT NULL
  GROUP BY s.shipment_id;

  SELECT CONCAT('✅ mart_shipments refreshed: ',
    CAST(COUNT(*) AS STRING), ' shipments')                       AS result
  FROM `logistics_dwh.mart_shipments`;
END;
