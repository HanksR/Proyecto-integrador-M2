FleetLogix – Data Engineering Project

Proyecto integral de Data Engineering para análisis logístico.
Incluye generación masiva de datos, análisis SQL, construcción de Data Warehouse en Snowflake y arquitectura cloud en AWS.

El proyecto está organizado por 4 avances progresivos, simulando un flujo real de ingeniería de datos end-to-end.

__________________________________________________________________________________
Estructura del Proyecto
FleetLogix-DataEngineering/
│
├── Avance1_DataGeneration/
│   ├── Data_generation.py
│   ├── generation_summary.json
│   ├── Validacion.sql
│   └── README.md
│
├── Avance2_SQLAnalysis/
│   ├── Queries_Analysis.sql
│   ├── Indices.ipynb
│   ├── Procesa.ipynb
│   └── README.md
│
├── Avance3_DataWarehouse/
│   ├── ETL_Pipeline.py
│   ├── Snowflake_ETL.sql
│   └── README.md
│
├── Avance4_CloudArchitecture/
│   ├── AWS_Lambda.py
│   ├── CloudArchitecture.py
│   └── README.md
│
├── config/
│   └── config.example.env
│
├── requirements.txt
├── .gitignore
└── README.md

____________________________________________________________________________________
Arquitectura del Proyecto

El flujo completo del sistema sigue estas etapas:

-> Generación de datos sintéticos en PostgreSQL
-> Validación de integridad y calidad de datos
-> Análisis SQL y optimización con índices
-> Pipeline ETL hacia Snowflake
-> Construcción de modelo dimensional (Star Schema)
-> Carga de tablas DIM y FACT
-> Cálculo de KPIs logísticos
-> Automatización programada del ETL
-> Integración con arquitectura Cloud en AWS

____________________________________________________________________
Avance 1 — Generación de Datos

Este módulo genera más de 500,000 registros sintéticos respetando reglas de negocio logísticas.

Datos generados:

Vehículos
Conductores
Rutas
Viajes
Entregas
Mantenimiento

Características:

Integridad referencial
Distribuciones realistas
Fechas coherentes
Validaciones automáticas
Reporte JSON de resumen

Ejecutar:

python Data_generation.py

____________________________________________________________________
Avance 2 — SQL Analysis

Este módulo contiene consultas analíticas y optimización del rendimiento.

Incluye:

Queries analíticas logísticas
KPIs operacionales
Análisis de eficiencia
Índices para optimización
Evaluación de performance
Notebook de procesamiento

Análisis realizados:

Entregas por ruta
Tiempo promedio de entrega
Eficiencia por vehículo
Consumo de combustible
Entregas por conductor
Costo logístico

____________________________________________________________________
Avance 3 — Data Warehouse

Pipeline ETL completo desde PostgreSQL hacia Snowflake.

Etapas:

1 Extracción desde PostgreSQL
2 Transformación de métricas logísticas
3 Construcción modelo dimensional
4 Carga dimensiones
5 Carga tabla de hechos
6 Cálculo KPIs agregados
7 Manejo SCD Type 2
8 Validaciones de calidad


python ETL_Pipeline.py

____________________________________________________________________
Avance 4 — Cloud Architecture

Arquitectura cloud para procesamiento logístico en tiempo real.

Componentes:

AWS Lambda
DynamoDB
S3
SNS Alerts
EventBridge Scheduler

Funciones implementadas:

Verificación de entregas
Cálculo ETA
Detección desvío de ruta
Alertas automáticas
Tracking vehículos
Tecnologías Util

___________________________________________________________________________
Autor
Proyecto de Data Engineering end-to-end para análisis logístico FleetLogix.