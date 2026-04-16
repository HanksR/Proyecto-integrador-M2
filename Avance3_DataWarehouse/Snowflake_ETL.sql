-- =========================================================
-- FLEETLOGIX DATA WAREHOUSE
-- Snowflake Dimensional Model - Production Ready
-- =========================================================

USE ROLE ACCOUNTADMIN;

-- =========================================================
-- 1. WAREHOUSE
-- =========================================================

CREATE WAREHOUSE IF NOT EXISTS FLEETLOGIX_WH
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- =========================================================
-- 2. DATABASE
-- =========================================================

CREATE DATABASE IF NOT EXISTS FLEETLOGIX_DW;

USE DATABASE FLEETLOGIX_DW;

-- =========================================================
-- 3. SCHEMAS (Arquitectura DW)
-- =========================================================

CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS MART;

-- =========================================================
-- 4. CORE LAYER (STAR SCHEMA)
-- =========================================================

USE SCHEMA CORE;

-- =========================================================
-- DIMENSION DATE
-- =========================================================

CREATE OR REPLACE TABLE dim_date (
    date_key INT,
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR,
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_num INT,
    month_name VARCHAR,
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR,
    fiscal_quarter INT,
    fiscal_year INT
)
CLUSTER BY (date_key);

-- =========================================================
-- DIMENSION TIME
-- =========================================================

CREATE OR REPLACE TABLE dim_time (
    time_key INT,
    hour INT,
    minute INT,
    second INT,
    time_of_day VARCHAR,
    hour_24 VARCHAR,
    hour_12 VARCHAR,
    am_pm VARCHAR,
    is_business_hour BOOLEAN,
    shift VARCHAR
)
CLUSTER BY (time_key);

-- =========================================================
-- DIMENSION VEHICLE (SCD TYPE 2)
-- =========================================================

CREATE OR REPLACE TABLE dim_vehicle (
    vehicle_key INT AUTOINCREMENT,
    vehicle_id INT,
    license_plate VARCHAR,
    vehicle_type VARCHAR,
    capacity_kg DECIMAL(10,2),
    fuel_type VARCHAR,
    acquisition_date DATE,
    age_months INT,
    status VARCHAR,
    last_maintenance_date DATE,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

-- =========================================================
-- DIMENSION DRIVER (SCD TYPE 2)
-- =========================================================

CREATE OR REPLACE TABLE dim_driver (
    driver_key INT AUTOINCREMENT,
    driver_id INT,
    employee_code VARCHAR,
    full_name VARCHAR,
    license_number VARCHAR,
    license_expiry DATE,
    phone VARCHAR,
    hire_date DATE,
    experience_months INT,
    status VARCHAR,
    performance_category VARCHAR,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

-- =========================================================
-- DIMENSION ROUTE
-- =========================================================

CREATE OR REPLACE TABLE dim_route (
    route_key INT AUTOINCREMENT,
    route_id INT,
    route_code VARCHAR,
    origin_city VARCHAR,
    destination_city VARCHAR,
    distance_km DECIMAL(10,2),
    estimated_duration_hours DECIMAL(5,2),
    toll_cost DECIMAL(10,2),
    difficulty_level VARCHAR,
    route_type VARCHAR
);

-- =========================================================
-- DIMENSION CUSTOMER
-- =========================================================

CREATE OR REPLACE TABLE dim_customer (
    customer_key INT AUTOINCREMENT,
    customer_id INT,
    customer_name VARCHAR,
    customer_type VARCHAR,
    city VARCHAR,
    first_delivery_date DATE,
    total_deliveries INT,
    customer_category VARCHAR
);

-- =========================================================
-- FACT TABLE
-- =========================================================

CREATE OR REPLACE TABLE fact_deliveries (

    delivery_key INT AUTOINCREMENT,

    date_key INT,
    scheduled_time_key INT,
    delivered_time_key INT,

    vehicle_key INT,
    driver_key INT,
    route_key INT,
    customer_key INT,

    delivery_id INT,
    trip_id INT,
    tracking_number VARCHAR,

    package_weight_kg DECIMAL(10,2),
    distance_km DECIMAL(10,2),
    fuel_consumed_liters DECIMAL(10,2),

    delivery_time_minutes INT,
    delay_minutes INT,

    deliveries_per_hour DECIMAL(10,2),
    fuel_efficiency_km_per_liter DECIMAL(10,2),
    cost_per_delivery DECIMAL(10,2),
    revenue_per_delivery DECIMAL(10,2),

    is_on_time BOOLEAN,
    is_damaged BOOLEAN,
    has_signature BOOLEAN,
    delivery_status VARCHAR,

    etl_batch_id INT,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()

)
CLUSTER BY (date_key, route_key);

-- =========================================================
-- AGGREGATE TABLES (PRE CALCULATED)
-- =========================================================

CREATE OR REPLACE TABLE daily_kpis (

    date_key INT,
    total_deliveries INT,
    total_revenue DECIMAL(12,2),
    avg_delivery_time DECIMAL(10,2),
    total_fuel DECIMAL(12,2),
    batch_id INT
);

CREATE OR REPLACE TABLE daily_summary (

    summary_date DATE,
    total_deliveries INT,
    successful_deliveries INT,
    failed_deliveries INT,
    avg_delivery_time_min DECIMAL(10,2),
    on_time_percentage DECIMAL(5,2),
    total_revenue DECIMAL(15,2),
    total_fuel_cost DECIMAL(15,2),
    etl_batch_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- =========================================================
-- STAGING LAYER (ETL INPUT)
-- =========================================================

USE SCHEMA STAGING;

CREATE OR REPLACE TABLE stg_fact_deliveries (

    date_key INT,
    scheduled_time_key INT,
    delivered_time_key INT,
    vehicle_key INT,
    driver_key INT,
    route_key INT,
    customer_key INT,

    delivery_id INT,
    trip_id INT,
    tracking_number VARCHAR,

    package_weight_kg DECIMAL(10,2),
    distance_km DECIMAL(10,2),
    fuel_consumed_liters DECIMAL(10,2),

    delivery_time_minutes INT,
    delay_minutes INT,

    deliveries_per_hour DECIMAL(10,2),
    fuel_efficiency_km_per_liter DECIMAL(10,2),
    cost_per_delivery DECIMAL(10,2),
    revenue_per_delivery DECIMAL(10,2),

    is_on_time BOOLEAN,
    is_damaged BOOLEAN,
    has_signature BOOLEAN,
    delivery_status VARCHAR,

    etl_batch_id INT
);

CREATE OR REPLACE TABLE stg_customer (
    customer_name VARCHAR,
    city VARCHAR
);

-- =========================================================
-- DATA RETENTION (TIME TRAVEL)
-- =========================================================

ALTER TABLE CORE.fact_deliveries SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE CORE.dim_driver SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE CORE.dim_vehicle SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE CORE.dim_customer SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE CORE.dim_route SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- =========================================================
-- MART LAYER (SECURE ANALYTICS VIEWS)
-- =========================================================

USE SCHEMA MART;

-- SALES VIEW
CREATE OR REPLACE SECURE VIEW v_sales_deliveries AS
SELECT
    d.full_date,
    c.customer_name,
    c.customer_type,
    f.package_weight_kg,
    f.delivery_status,
    f.revenue_per_delivery
FROM CORE.fact_deliveries f
JOIN CORE.dim_date d ON f.date_key = d.date_key
JOIN CORE.dim_customer c ON f.customer_key = c.customer_key;

-- OPERATIONS VIEW
CREATE OR REPLACE SECURE VIEW v_operations_deliveries AS
SELECT
    d.full_date,
    t.hour_24 AS hora,
    v.license_plate,
    dr.full_name AS conductor,
    r.route_code,
    c.customer_name,
    f.delivery_time_minutes,
    f.delay_minutes,
    f.is_on_time,
    f.fuel_consumed_liters
FROM CORE.fact_deliveries f
JOIN CORE.dim_date d ON f.date_key = d.date_key
JOIN CORE.dim_time t ON f.scheduled_time_key = t.time_key
JOIN CORE.dim_vehicle v ON f.vehicle_key = v.vehicle_key
JOIN CORE.dim_driver dr ON f.driver_key = dr.driver_key
JOIN CORE.dim_route r ON f.route_key = r.route_key
JOIN CORE.dim_customer c ON f.customer_key = c.customer_key;

-- =========================================================
-- ROLES
-- =========================================================

CREATE ROLE IF NOT EXISTS SALES_ANALYST;
CREATE ROLE IF NOT EXISTS OPERATIONS_ANALYST;
CREATE ROLE IF NOT EXISTS FLEETLOGIX_ETL;

-- =========================================================
-- PERMISSIONS
-- =========================================================

GRANT USAGE ON WAREHOUSE FLEETLOGIX_WH TO ROLE SALES_ANALYST;
GRANT USAGE ON WAREHOUSE FLEETLOGIX_WH TO ROLE OPERATIONS_ANALYST;
GRANT USAGE ON WAREHOUSE FLEETLOGIX_WH TO ROLE FLEETLOGIX_ETL;

GRANT USAGE ON DATABASE FLEETLOGIX_DW TO ROLE SALES_ANALYST;
GRANT USAGE ON DATABASE FLEETLOGIX_DW TO ROLE OPERATIONS_ANALYST;
GRANT USAGE ON DATABASE FLEETLOGIX_DW TO ROLE FLEETLOGIX_ETL;

GRANT USAGE ON SCHEMA FLEETLOGIX_DW.MART TO ROLE SALES_ANALYST;
GRANT USAGE ON SCHEMA FLEETLOGIX_DW.MART TO ROLE OPERATIONS_ANALYST;

GRANT SELECT ON ALL VIEWS IN SCHEMA MART TO ROLE SALES_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA MART TO ROLE OPERATIONS_ANALYST;

-- ETL ACCESS

GRANT USAGE ON SCHEMA FLEETLOGIX_DW.CORE TO ROLE FLEETLOGIX_ETL;
GRANT USAGE ON SCHEMA FLEETLOGIX_DW.STAGING TO ROLE FLEETLOGIX_ETL;

GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CORE TO ROLE FLEETLOGIX_ETL;
GRANT INSERT ON ALL TABLES IN SCHEMA STAGING TO ROLE FLEETLOGIX_ETL;



SELECT COUNT(*)
FROM FLEETLOGIX_DW.CORE.DIM_CUSTOMER

CREATE OR REPLACE TABLE FLEETLOGIX_DW.STAGING.staging_daily_load (
    raw_data VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

