-- ==========================================
-- SEED: customers (20 dummy customers)
-- ==========================================
INSERT INTO logistics.customers (full_name, email, phone, city, province) VALUES
('Budi Santoso',       'budi.santoso@gmail.com',    '081234567890', 'Jakarta',    'DKI Jakarta'),
('Siti Rahayu',        'siti.rahayu@gmail.com',     '081234567891', 'Surabaya',   'Jawa Timur'),
('Ahmad Fauzi',        'ahmad.fauzi@gmail.com',     '081234567892', 'Bandung',    'Jawa Barat'),
('Dewi Lestari',       'dewi.lestari@gmail.com',    '081234567893', 'Medan',      'Sumatera Utara'),
('Rizky Pratama',      'rizky.pratama@gmail.com',   '081234567894', 'Yogyakarta', 'DI Yogyakarta'),
('Nur Fadilah',        'nur.fadilah@gmail.com',     '081234567895', 'Semarang',   'Jawa Tengah'),
('Hendra Wijaya',      'hendra.wijaya@gmail.com',   '081234567896', 'Makassar',   'Sulawesi Selatan'),
('Rina Marlina',       'rina.marlina@gmail.com',    '081234567897', 'Palembang',  'Sumatera Selatan'),
('Doni Setiawan',      'doni.setiawan@gmail.com',   '081234567898', 'Balikpapan', 'Kalimantan Timur'),
('Fitri Handayani',    'fitri.handayani@gmail.com', '081234567899', 'Denpasar',   'Bali'),
('Agus Permana',       'agus.permana@gmail.com',    '081234567800', 'Tangerang',  'Banten'),
('Yuli Astuti',        'yuli.astuti@gmail.com',     '081234567801', 'Bekasi',     'Jawa Barat'),
('Fajar Nugroho',      'fajar.nugroho@gmail.com',   '081234567802', 'Depok',      'Jawa Barat'),
('Lina Susanti',       'lina.susanti@gmail.com',    '081234567803', 'Bogor',      'Jawa Barat'),
('Tono Hartono',       'tono.hartono@gmail.com',    '081234567804', 'Malang',     'Jawa Timur'),
('Maya Sari',          'maya.sari@gmail.com',       '081234567805', 'Pekanbaru',  'Riau'),
('Eko Wahyudi',        'eko.wahyudi@gmail.com',     '081234567806', 'Batam',      'Kepulauan Riau'),
('Sari Dewi',          'sari.dewi@gmail.com',       '081234567807', 'Manado',     'Sulawesi Utara'),
('Bambang Susilo',     'bambang.susilo@gmail.com',  '081234567808', 'Pontianak',  'Kalimantan Barat'),
('Indah Permata',      'indah.permata@gmail.com',   '081234567809', 'Lombok',     'NTB');

-- ==========================================
-- SEED: orders (10 dummy orders)
-- ==========================================
INSERT INTO logistics.orders (customer_id, order_status, total_amount, payment_method, order_date) VALUES
(1,  'DELIVERED',   4299000,  'TRANSFER',    NOW() - INTERVAL '10 days'),
(2,  'SHIPPED',     8499000,  'CREDIT_CARD', NOW() - INTERVAL '7 days'),
(3,  'PROCESSING',  2448000,  'GOPAY',       NOW() - INTERVAL '5 days'),
(4,  'PENDING',      189000,  'COD',         NOW() - INTERVAL '3 days'),
(5,  'DELIVERED',   6898000,  'OVO',         NOW() - INTERVAL '12 days'),
(6,  'CANCELLED',    549000,  'TRANSFER',    NOW() - INTERVAL '8 days'),
(7,  'SHIPPED',     5588000,  'DANA',        NOW() - INTERVAL '4 days'),
(8,  'PROCESSING',   838000,  'GOPAY',       NOW() - INTERVAL '2 days'),
(9,  'DELIVERED',  13548000,  'CREDIT_CARD', NOW() - INTERVAL '15 days'),
(10, 'PENDING',      318000,  'COD',         NOW() - INTERVAL '1 day');

-- ==========================================
-- SEED: order_items
-- ==========================================
INSERT INTO logistics.order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES
(1,  1,  1, 4299000,  4299000),
(2,  2,  1, 8499000,  8499000),
(3,  3,  1, 1899000,  1899000),
(3,  4,  1,  549000,   549000),
(4,  5,  1,  189000,   189000),
(5,  3,  1, 1899000,  1899000),
(5,  6,  1, 4999000,  4999000),
(6,  4,  1,  549000,   549000),
(7,  6,  1, 4999000,  4999000),
(7,  11, 1,  199000,   199000),
(7,  12, 2,   89000,   178000),
(8,  11, 2,  199000,   398000),
(8,  12, 2,   89000,   178000),
(8,  15, 2,  129000,   258000),
(9,  10, 1,12999000, 12999000),
(9,  13, 1, 1299000,  1299000),
(10, 12, 2,   89000,   178000),
(10, 15, 1,  129000,   129000);

-- ==========================================
-- SEED: shipments
-- ==========================================
INSERT INTO logistics.shipments (order_id, courier, tracking_number, origin_city, destination_city, shipment_status, shipped_at, estimated_arrival) VALUES
(1,  'JNE',      'JNE20240001', 'Jakarta',    'Jakarta',    'DELIVERED',      NOW() - INTERVAL '9 days',  NOW() - INTERVAL '7 days'),
(2,  'SiCepat',  'SCP20240002', 'Surabaya',   'Surabaya',   'IN_TRANSIT',     NOW() - INTERVAL '6 days',  NOW() + INTERVAL '1 day'),
(3,  'JNT',      'JNT20240003', 'Bandung',    'Bandung',    'WAITING_PICKUP', NULL,                        NOW() + INTERVAL '3 days'),
(5,  'Anteraja', 'ANT20240005', 'Yogyakarta', 'Yogyakarta', 'DELIVERED',      NOW() - INTERVAL '11 days', NOW() - INTERVAL '9 days'),
(7,  'JNE',      'JNE20240007', 'Makassar',   'Makassar',   'IN_TRANSIT',     NOW() - INTERVAL '3 days',  NOW() + INTERVAL '2 days'),
(9,  'TIKI',     'TIK20240009', 'Balikpapan', 'Balikpapan', 'DELIVERED',      NOW() - INTERVAL '14 days', NOW() - INTERVAL '12 days');

-- ==========================================
-- SEED: delivery_events
-- ==========================================
INSERT INTO logistics.delivery_events (shipment_id, event_type, event_location, event_note, event_time) VALUES
(1, 'PICKUP',           'Jakarta Selatan',  'Paket diambil kurir',              NOW() - INTERVAL '9 days'),
(1, 'IN_TRANSIT',       'Jakarta Pusat',    'Paket tiba di hub Jakarta',        NOW() - INTERVAL '8 days'),
(1, 'OUT_FOR_DELIVERY', 'Jakarta Utara',    'Paket sedang diantar',             NOW() - INTERVAL '7 days'),
(1, 'DELIVERED',        'Jakarta Utara',    'Paket diterima oleh penerima',     NOW() - INTERVAL '7 days'),
(2, 'PICKUP',           'Surabaya Barat',   'Paket diambil kurir SiCepat',      NOW() - INTERVAL '6 days'),
(2, 'IN_TRANSIT',       'Surabaya Timur',   'Paket dalam perjalanan',           NOW() - INTERVAL '5 days'),
(4, 'PICKUP',           'Yogyakarta',       'Paket diambil kurir Anteraja',     NOW() - INTERVAL '11 days'),
(4, 'IN_TRANSIT',       'Solo',             'Paket transit di Solo',            NOW() - INTERVAL '10 days'),
(4, 'DELIVERED',        'Yogyakarta',       'Paket diterima',                   NOW() - INTERVAL '9 days'),
(5, 'PICKUP',           'Makassar',         'Paket diambil kurir JNE',          NOW() - INTERVAL '3 days'),
(5, 'IN_TRANSIT',       'Makassar Hub',     'Paket di sortir hub Makassar',     NOW() - INTERVAL '2 days'),
(6, 'PICKUP',           'Balikpapan',       'Paket diambil kurir TIKI',         NOW() - INTERVAL '14 days'),
(6, 'IN_TRANSIT',       'Samarinda',        'Transit Samarinda',                NOW() - INTERVAL '13 days'),
(6, 'OUT_FOR_DELIVERY', 'Balikpapan',       'Paket sedang diantar ke alamat',   NOW() - INTERVAL '12 days'),
(6, 'DELIVERED',        'Balikpapan',       'Paket diterima penerima',          NOW() - INTERVAL '12 days');
