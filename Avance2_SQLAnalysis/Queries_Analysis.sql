


-- Query 1: Vehículos activos con su último mantenimiento
-- Nivel: Básico
-- Problema de negocio:
-- Identificar qué vehículos están operativos y cuándo fue su último mantenimiento
-- para prevenir fallas y planificar mantenimientos futuros.

SELECT 
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type,
    v.status,
    MAX(m.maintenance_date) AS last_maintenance_date
FROM vehicles v
LEFT JOIN maintenance m 
    ON v.vehicle_id = m.vehicle_id
WHERE v.status = 'active'
GROUP BY v.vehicle_id, v.license_plate, v.vehicle_type, v.status
ORDER BY last_maintenance_date DESC;


select COUNT(status) from vehicles


-- EXPLAIN ANALYZE
SELECT 
    v.vehicle_id,
    v.license_plate,
    v.vehicle_type,
    v.status,
    MAX(m.maintenance_date) AS last_maintenance_date
FROM vehicles v
LEFT JOIN maintenance m 
    ON v.vehicle_id = m.vehicle_id
WHERE v.status = 'active'
GROUP BY v.vehicle_id, v.license_plate, v.vehicle_type, v.status
ORDER BY last_maintenance_date DESC;



-- _____________________________________________________________________________________________

-- Query 2: Número de viajes por conductor
-- Nivel: Básico
-- Problema de negocio:
-- Evaluar la carga de trabajo de los conductores para identificar
-- posibles sobrecargas o subutilización.

SELECT 
    d.driver_id,
    d.first_name,
    d.last_name,
    COUNT(t.trip_id) AS total_trips
FROM drivers d
LEFT JOIN trips t 
    ON d.driver_id = t.driver_id
GROUP BY d.driver_id, d.first_name, d.last_name
ORDER BY total_trips DESC;



-- EXPLAIN ANALYZE
SELECT 
    d.driver_id,
    d.first_name,
    d.last_name,
    COUNT(t.trip_id) AS total_trips
FROM drivers d
LEFT JOIN trips t 
    ON d.driver_id = t.driver_id
GROUP BY d.driver_id, d.first_name, d.last_name
ORDER BY total_trips DESC;



-- _____________________________________________________________________________________________


-- Query 3: Promedio de peso transportado por tipo de vehículo
-- Nivel: Básico
-- Problema de negocio:
-- Analizar si los vehículos están siendo utilizados eficientemente
-- en función de su capacidad.

SELECT 
    v.vehicle_type,
    AVG(t.total_weight_kg) AS avg_weight,
    COUNT(t.trip_id) AS total_trips
FROM vehicles v
JOIN trips t 
    ON v.vehicle_id = t.vehicle_id
GROUP BY v.vehicle_type
ORDER BY avg_weight DESC;


-- EXPLAIN ANALYZE
SELECT 
    v.vehicle_type,
    AVG(t.total_weight_kg) AS avg_weight,
    COUNT(t.trip_id) AS total_trips
FROM vehicles v
JOIN trips t 
    ON v.vehicle_id = t.vehicle_id
GROUP BY v.vehicle_type
ORDER BY avg_weight DESC;


-- _____________________________________________________________________________________________
-- QUERY 4 – Promedio de entregas por conductor (últimos 6 meses)

-- EXPLAIN ANALYZE
SELECT 
    d.driver_id,
    d.first_name,
    d.last_name,
    COUNT(del.delivery_id) AS total_deliveries,
    COUNT(DISTINCT t.trip_id) AS total_trips
    -- COUNT(del.delivery_id)::decimal / COUNT(DISTINCT t.trip_id) AS avg_deliveries_per_trip
FROM drivers d
JOIN trips t ON d.driver_id = t.driver_id
JOIN deliveries del ON t.trip_id = del.trip_id
-- WHERE t.departure_datetime >= NOW() - INTERVAL '6 months'
GROUP BY d.driver_id, d.first_name, d.last_name
-- HAVING COUNT(DISTINCT t.trip_id) > 0
ORDER BY avg_deliveries_per_trip DESC;


-- _____________________________________________________________________________________________
-- QUERY 5 – Rutas más utilizadas


-- EXPLAIN ANALYZE
SELECT 
    r.route_id,
    r.origin_city,
    r.destination_city,
    COUNT(t.trip_id) AS total_trips
FROM routes r
JOIN trips t ON r.route_id = t.route_id
GROUP BY r.route_id, r.origin_city, r.destination_city
ORDER BY total_trips DESC;


-- _____________________________________________________________________________________________
-- QUERY 6 – conductores con más entregas (HAVING)

-- EXPLAIN ANALYZE
SELECT 
    d.driver_id,
    d.first_name,
    d.last_name,
    COUNT(del.delivery_id) AS total_deliveries
FROM drivers d
JOIN trips t ON d.driver_id = t.driver_id
JOIN deliveries del ON t.trip_id = del.trip_id
GROUP BY d.driver_id, d.first_name, d.last_name
HAVING COUNT(del.delivery_id) > 1000
ORDER BY total_deliveries DESC;



-- _____________________________________________________________________________________________
-- QUERY 7 – Entregas con retraso

-- EXPLAIN ANALYZE
SELECT 
    delivery_id,
    trip_id,
    scheduled_datetime,
    delivered_datetime,
    (delivered_datetime - scheduled_datetime) AS delay_time
FROM deliveries
WHERE delivered_datetime IS NOT NULL
AND delivered_datetime > scheduled_datetime
ORDER BY delay_time DESC;


-- _____________________________________________________________________________________________
-- QUERY 8 – Consumo promedio de combustible por ruta

-- EXPLAIN ANALYZE
SELECT 
    r.route_id,
    r.origin_city,
    r.destination_city,
    AVG(t.fuel_consumed_liters) AS avg_fuel_consumption,
    COUNT(t.trip_id) AS total_trips
FROM routes r
JOIN trips t ON r.route_id = t.route_id
WHERE t.status = 'completed'
GROUP BY r.route_id, r.origin_city, r.destination_city
ORDER BY avg_fuel_consumption DESC;



-- _____________________________________________________________________________________________
-- QUERY 9 – Ranking de eficiencia por uso de capacidad (Window Function)

-- EXPLAIN ANALYZE
SELECT 
    v.vehicle_type,
    t.trip_id,
    t.total_weight_kg,
    v.capacity_kg,
    (t.total_weight_kg / v.capacity_kg) AS utilization_rate,
    RANK() OVER (
        PARTITION BY v.vehicle_type 
        ORDER BY (t.total_weight_kg / v.capacity_kg) DESC
    ) AS efficiency_rank
FROM trips t
JOIN vehicles v ON t.vehicle_id = v.vehicle_id
WHERE v.capacity_kg > 0;



-- _____________________________________________________________________________________________
-- QUERY 10 – Costo total de mantenimiento por vehículo vs uso (CTE)

-- EXPLAIN ANALYZE
WITH vehicle_usage AS (
    SELECT 
        v.vehicle_id,
        COUNT(t.trip_id) AS total_trips
    FROM vehicles v
    LEFT JOIN trips t ON v.vehicle_id = t.vehicle_id
    GROUP BY v.vehicle_id
),
maintenance_costs AS (
    SELECT 
        vehicle_id,
        SUM(cost) AS total_maintenance_cost
    FROM maintenance
    GROUP BY vehicle_id
)
SELECT 
    v.vehicle_id,
    v.vehicle_type,
    vu.total_trips,
    mc.total_maintenance_cost,
    (mc.total_maintenance_cost / NULLIF(vu.total_trips, 0)) AS cost_per_trip
FROM vehicles v
LEFT JOIN vehicle_usage vu ON v.vehicle_id = vu.vehicle_id
LEFT JOIN maintenance_costs mc ON v.vehicle_id = mc.vehicle_id
ORDER BY cost_per_trip;

-- _____________________________________________________________________________________________
-- QUERY 11 – Subconsulta correlacionada: entregas por encima del promedio del viaje




-- EXPLAIN ANALYZE
SELECT 
    d.delivery_id,
    d.trip_id,
    d.package_weight_kg
FROM deliveries d
WHERE d.package_weight_kg > (
    SELECT AVG(d2.package_weight_kg)
    FROM deliveries d2
    WHERE d2.trip_id = d.trip_id
);



-- _____________________________________________________________________________________________
-- QUERY 12 – “PIVOT” de entregas por estado (simulación con CASE)

-- EXPLAIN ANALYZE
SELECT 
    t.trip_id,
    COUNT(CASE WHEN d.delivery_status = 'delivered' THEN 1 END) AS delivered_count,
    COUNT(CASE WHEN d.delivery_status = 'pending' THEN 1 END) AS pending_count,
    COUNT(CASE WHEN d.recipient_signature = TRUE THEN 1 END) AS signed_count
FROM trips t
JOIN deliveries d ON t.trip_id = d.trip_id
GROUP BY t.trip_id
ORDER BY delivered_count DESC;







