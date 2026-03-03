from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# Variables (Airflow Variables)
# =========================
# Crea estas variables en Composer:
# - wasi_project_id
# - wasi_region
# - wasi_bucket
# - wasi_dataset
# - wasi_bq_location
#
# Ejemplo:
# wasi_project_id = project-53930c9b-4e21-4329-b74
# wasi_region     = us-central1
# wasi_bucket     = wasi-qollqui-lake-53930c9b
# wasi_dataset    = wasi_qollqui
# wasi_bq_location= us-central1

PROJECT_ID = Variable.get("wasi_project_id")
REGION = Variable.get("wasi_region", default_var="us-central1")
BUCKET = Variable.get("wasi_bucket")
DATASET = Variable.get("wasi_dataset", default_var="wasi_qollqui")
BQ_LOCATION = Variable.get("wasi_bq_location", default_var="us-central1")

# Rutas en GCS según tu medallón
BRONZE_PREFIX = "bronze/csv/"  # objetos: gs://BUCKET/bronze/csv/*.csv
CODE_OBJECT = "code/pipeline_wasi_qollqui.py"  # gs://BUCKET/code/pipeline_wasi_qollqui.py

# CSV esperados
REQUIRED_CSVS = [
    "clientes.csv",
    "deuda.csv",
    "pagos.csv",
    "gestiones_cobranza.csv",
    "productos.csv",
    "promesas_pago.csv",
]

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="wasi_qollqui_medallion_dataproc_bq",
    description="Orquesta Bronze->Silver->Gold (Parquet) con Dataproc Serverless y materializa BI en BigQuery",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # o "0 2 * * *" para diario 2am
    catchup=False,
    tags=["wasi-qollqui", "dataproc", "bigquery", "medallion"],
) as dag:

    # 1) Sensor: asegurar que existan los CSV
    wait_for_bronze = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_bronze_csvs",
        bucket=BUCKET,
        prefix=BRONZE_PREFIX,
        # El sensor valida que exista ALGO con ese prefijo.
        # Abajo validamos explícitamente los 6 archivos via una query BQ opcional o list en GCS,
        # pero para mantenerlo simple, el prefijo suele bastar.
        timeout=60 * 10,
        poke_interval=30,
        mode="poke",
    )

    # 2) Dataproc Serverless Batch: ejecuta el script en GCS
    #    IMPORTANTE: Esto corre con el Service Account de Composer/Worker,
    #    asegúrate que tenga permisos Dataproc + GCS + BigQuery (si tu script escribe a BQ).
    run_dataproc_batch = DataprocCreateBatchOperator(
        task_id="run_dataproc_serverless_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"wasi-qollqui-{{{{ ds_nodash }}}}-{{{{ ts_nodash }}}}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET}/{CODE_OBJECT}",
                "args": [],
                "properties": {
                    # tus configs Spark custom:
                    "spark.wasi.bucket": BUCKET,
                    "spark.wasi.project": PROJECT_ID,
                    "spark.wasi.dataset": DATASET,
                    "spark.wasi.bq_location": BQ_LOCATION,
                },
            },
            "environment_config": {
                # opcional: tuning
                # "execution_config": {"service_account": "..."},
            },
            "runtime_config": {
                "version": "2.2",  # Dataproc runtime
            },
        },
    )

    # 3) BigQuery: Crear dataset si no existe
    #    Usamos DDL IF NOT EXISTS (simple y robusto)
    create_dataset = BigQueryInsertJobOperator(
        task_id="create_dataset_if_not_exists",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET}`",
                "useLegacySql": False,
            }
        },
    )

    # 4) Materializar tabla BI desde la tabla origen (que tu pipeline crea)
    materialize_kpi_cliente_bi = BigQueryInsertJobOperator(
        task_id="materialize_kpi_cliente_bi",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.kpi_cliente_bi` AS
                SELECT * FROM `{PROJECT_ID}.{DATASET}.kpi_cliente`;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Orden de ejecución
    wait_for_bronze >> run_dataproc_batch >> create_dataset >> materialize_kpi_cliente_bi