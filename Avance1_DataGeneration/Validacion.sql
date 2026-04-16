


-- ____________________________________________________________________________________________________
-- 1. VALIDACIÓN DE INTEGRIDAD REFERENCIAL

SELECT COUNT(*) AS invalid_trips
FROM trips t
LEFT JOIN vehicles v ON t.vehicle_id = v.vehicle_id
WHERE v.vehicle_id IS NULL;



SELECT COUNT(*) AS invalid_trips
FROM trips t
LEFT JOIN vehicles v ON t.vehicle_id = v.vehicle_id
WHERE v.vehicle_id IS NULL;


SELECT COUNT(*) AS invalid_maintenance
FROM maintenance m
LEFT JOIN vehicles v ON m.vehicle_id = v.vehicle_id
WHERE v.vehicle_id IS NULL;

-- ____________________________________________________________________________________________________
-- 2. CONSISTENCIA TEMPORAL

SELECT COUNT(*) AS invalid_time_records
FROM trips
WHERE arrival_datetime IS NOT NULL
AND arrival_datetime < departure_datetime;


SELECT COUNT(*) AS invalid_delivery_times
FROM deliveries
WHERE delivered_datetime IS NOT NULL
AND delivered_datetime < scheduled_datetime;



-- ____________________________________________________________________________________________________
-- 3. VALIDACIÓN DE DISTRIBUCIONES ESTADÍSTICAS

SELECT 
    AVG(delivery_count) AS avg_deliveries,
    MIN(delivery_count) AS min_deliveries,
    MAX(delivery_count) AS max_deliveries
FROM (
    SELECT trip_id, COUNT(*) AS delivery_count
    FROM deliveries
    GROUP BY trip_id
) t;


SELECT 
    EXTRACT(HOUR FROM departure_datetime) AS hour,
    COUNT(*) AS total_trips
FROM trips
GROUP BY hour
ORDER BY hour;



SELECT COUNT(*) AS overload_trips
FROM trips t
JOIN vehicles v ON t.vehicle_id = v.vehicle_id
WHERE t.total_weight_kg > v.capacity_kg;



-- ____________________________________________________________________________________________________
-- 4. VALIDACIÓN FINAL DE VOLUMEN DE DATOS


SELECT 'vehicles' AS table_name, COUNT(*) FROM vehicles
UNION ALL
SELECT 'drivers', COUNT(*) FROM drivers
UNION ALL
SELECT 'routes', COUNT(*) FROM routes
UNION ALL
SELECT 'trips', COUNT(*) FROM trips
UNION ALL
SELECT 'deliveries', COUNT(*) FROM deliveries
UNION ALL
SELECT 'maintenance', COUNT(*) FROM maintenance;
