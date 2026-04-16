"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import schedule
import time
import json
from typing import Dict, List, Tuple
import os 

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
POSTGRES_CONFIG = {
    'host': os.getenv("POSTGRES_HOST"),
    'database': os.getenv("POSTGRES_DB"),
    'user': os.getenv("POSTGRES_USER"),
    'password': os.getenv("POSTGRES_PASSWORD"),
    'port': os.getenv("POSTGRES_PORT")
}

SNOWFLAKE_CONFIG = {
    'user': os.getenv("SNOWFLAKE_USER"),
    'password': os.getenv("SNOWFLAKE_PASSWORD"),
    'account': os.getenv("SNOWFLAKE_ACCOUNT"),
    'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
    'database': os.getenv("SNOWFLAKE_DATABASE"),
    'schema': os.getenv("SNOWFLAKE_SCHEMA"),
    'role': os.getenv("SNOWFLAKE_ROLE")
}

class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }
    
    def connect_databases(self):
        try:
           logging.info("Conectando PostgreSQL...")
           self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
           self.pg_conn.set_client_encoding('LATIN1')
           logging.info("OK PostgreSQL")

           logging.info("Conectando Snowflake...")
           self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
           logging.info("OK Snowflake")

           return True

        except Exception as e:
           logging.error(f"Error en conexión: {e}")
           return False
    


    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos del día anterior de PostgreSQL"""
        logging.info(" Iniciando extracción de datos...")
        
        query = """
        SELECT 
        d.delivery_id,
        d.trip_id,
        d.tracking_number,
        d.customer_name,
        d.package_weight_kg,
        d.scheduled_datetime,
        d.delivered_datetime,
        d.delivery_status,
        d.recipient_signature,
        
        t.vehicle_id,
        t.driver_id,
        t.route_id,
        t.departure_datetime,
        t.arrival_datetime,
        t.fuel_consumed_liters,
        
        r.distance_km,
        r.toll_cost,
        r.destination_city
        
    FROM deliveries d
    JOIN trips t ON d.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id

    WHERE t.departure_datetime >= '2026-02-16' -- Cambiar a fecha dinámica para producción
    """
        
        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f" Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info(" Iniciando transformación de datos...")
        
        try:
            # Calcular métricas
            df['delivery_time_minutes'] = (
                (pd.to_datetime(df['delivered_datetime']) - 
                 pd.to_datetime(df['scheduled_datetime'])).dt.total_seconds() / 60
            ).round(2)
            
            df['delay_minutes'] = df['delivery_time_minutes'].apply(
                lambda x: max(0, x) if x > 0 else 0
            )
            
            df['is_on_time'] = df['delay_minutes'] <= 30
            
            # Calcular entregas por hora
            df['trip_duration_hours'] = (
                (pd.to_datetime(df['arrival_datetime']) - 
                 pd.to_datetime(df['departure_datetime'])).dt.total_seconds() / 3600
            ).round(2)
            
            # Agrupar entregas por trip para calcular entregas/hora
            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
            df['deliveries_per_hour'] = (
                df['deliveries_in_trip'] / df['trip_duration_hours']
            ).round(2)
            
            # Eficiencia de combustible
            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km'] / df['fuel_consumed_liters']
            ).round(2)
            
            # Costo estimado por entrega
            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'] * 5000 + df['toll_cost']) / 
                df['deliveries_in_trip']
            ).round(2)
            
            # Revenue estimado (ejemplo: $20,000 base + $500 por kg)
            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)
            
            # Validaciones de calidad
            # No permitir tiempos negativos
            df = df[df['delivery_time_minutes'] >= 0]
            
            # No permitir pesos fuera de rango
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]
            
            # Manejar cambios históricos (SCD Type 2 para conductor/vehículo)
            df['valid_from'] = pd.to_datetime(df['scheduled_datetime']).dt.date
            df['valid_to'] = pd.to_datetime('9999-12-31').date() # Corregir ------- 
            df['is_current'] = True
            
            self.metrics['records_transformed'] = len(df)
            logging.info(f" Transformados {len(df)} registros")
            
            return df
            
        except Exception as e:
            logging.error(f" Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    

    
    def load_dimensions(self, df: pd.DataFrame):
        logging.info("Cargando dimensiones (OPTIMIZADO)...")

        cursor = self.sf_conn.cursor()

        try:
          
            customers = df[['customer_name', 'destination_city']].drop_duplicates()

            cursor.executemany("""
                MERGE INTO dim_customer c
                USING (
                    SELECT %s AS customer_name,
                        %s AS city
                ) s
                ON c.customer_name = s.customer_name
                WHEN NOT MATCHED THEN
                INSERT (
                    customer_name,
                    customer_type,
                    city,
                    first_delivery_date,
                    total_deliveries,
                    customer_category
                )
                VALUES (
                    s.customer_name,
                    'Individual',
                    s.city,
                    CURRENT_DATE(),
                    0,
                    'Regular'
                )
            """, customers.values.tolist())

            # =========================
            # SCD TYPE 2 (FIX IMPORTANTE)
            # =========================
            cursor.execute("""
                UPDATE dim_driver 
                SET valid_to = CURRENT_DATE() - 1,
                    is_current = FALSE
                WHERE is_current = TRUE
            """)

            self.sf_conn.commit()
            logging.info("Dimensiones cargadas correctamente (BULK)")

        except Exception as e:
            logging.error(f"Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1
    



    def load_facts(self, df: pd.DataFrame):
        logging.info("Cargando facts (OPTIMIZADO)...")

        cursor = self.sf_conn.cursor()

        try:
            df['date_key'] = pd.to_datetime(df['scheduled_datetime']).dt.strftime('%Y%m%d').astype(int)
            df['scheduled_time_key'] = pd.to_datetime(df['scheduled_datetime']).dt.hour * 100
            df['delivered_time_key'] = pd.to_datetime(df['delivered_datetime']).dt.hour * 100

            fact_data = df[[
                'date_key',
                'scheduled_time_key',
                'delivered_time_key',
                'vehicle_id',
                'driver_id',
                'route_id',
                'delivery_id',
                'trip_id',
                'tracking_number',
                'package_weight_kg',
                'distance_km',
                'fuel_consumed_liters',
                'delivery_time_minutes',
                'delay_minutes',
                'deliveries_per_hour',
                'fuel_efficiency_km_per_liter',
                'cost_per_delivery',
                'revenue_per_delivery',
                'is_on_time',
                'recipient_signature',
                'delivery_status'
            ]].copy()

            fact_data['customer_key'] = 1
            fact_data['is_damaged'] = False
            fact_data['etl_batch_id'] = self.batch_id

            cursor.executemany("""
                INSERT INTO fact_deliveries (
                    date_key, scheduled_time_key, delivered_time_key,
                    vehicle_key, driver_key, route_key,
                    delivery_id, trip_id, tracking_number,
                    package_weight_kg, distance_km, fuel_consumed_liters,
                    delivery_time_minutes, delay_minutes,
                    deliveries_per_hour, fuel_efficiency_km_per_liter,
                    cost_per_delivery, revenue_per_delivery,
                    is_on_time, has_signature, delivery_status,
                    customer_key, is_damaged, etl_batch_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, fact_data.values.tolist())

            self.sf_conn.commit()
            logging.info(f"Facts cargados: {len(fact_data)}")

        except Exception as e:
            logging.error(f"Error facts: {e}")
            self.sf_conn.rollback()



    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f" Iniciando ETL - Batch ID: {self.batch_id}")
        
        try:
            # Conectar
            if not self.connect_databases():
                return
            
            # ETL
            df = self.extract_daily_data()
            if not df.empty:
                df_transformed = self.transform_data(df)
                if not df_transformed.empty:
                    self.load_dimensions(df_transformed)
                    self.load_facts(df_transformed)
            
            # Calcular totales para reportes
            self._calculate_daily_totals()
            
            # Cerrar conexiones
            self.close_connections()
            
            # Log final
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f" ETL completado en {duration:.2f} segundos")
            logging.info(f" Métricas: {json.dumps(self.metrics, indent=2)}")
            
        except Exception as e:
            logging.error(f" Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()
    
    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()
        
        try:
            # Crear tabla de totales si no existe
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_kpis (
            date_key INT,
              total_deliveries INT,
              total_revenue DECIMAL(12,2),
              avg_delivery_time DECIMAL(10,2),
              total_fuel DECIMAL(12,2),
              batch_id INT) 
                           """)
            
            # Insertar totales del día
            cursor.execute("""
              INSERT INTO daily_kpis
              SELECT 
                date_key,
                COUNT(*) as total_deliveries,
                SUM(revenue_per_delivery),
                AVG(delivery_time_minutes),
                SUM(fuel_consumed_liters),
                %s
                FROM fact_deliveries
                WHERE etl_batch_id = %s
                GROUP BY date_key
                """, (self.batch_id,self.batch_id))
            
            self.sf_conn.commit()
            logging.info(" Totales diarios calculados")
            
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")
    
    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info(" Conexiones cerradas")

def job():
    """Función para programar con schedule"""
    etl = FleetLogixETL()
    etl.run_etl()

def main():
    """Función principal - Automatización diaria"""
    logging.info(" Pipeline ETL FleetLogix iniciado")
    
    # Programar ejecución diaria a las 2:00 AM
    schedule.every().day.at("02:00").do(job)
    
    logging.info(" ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")
    
    # Ejecutar una vez al inicio (para pruebas)
    job()
    
    # Loop infinito esperando la hora programada
    while True:
        schedule.run_pending()
        time.sleep(60)  # Verificar cada minuto

if __name__ == "__main__":
    main()


    # CORREGIR .date()
    # DIM_CUSTOMER (SERIAL - INDETITY)  snowflake



