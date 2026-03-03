from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# =========
# Ajusta estos 3 valores si cambiaste algo
# =========
PROJECT_ID = "project-53930c9b-4e21-4329-b74"
REGION = "us-central1"
BUCKET = "wasi-qollqui-datalake-2"     # tu datalake (bronze/silver/gold)
DATASET = "wasi_qollqui2"              # tu dataset en BigQuery

# Script que ya subiste a GCS (Dataproc Serverless lo ejecuta)
PYSPARK_URI = f"gs://{BUCKET}/jobs/pipeline_wasi_qollqui.py"

DEFAULT_ARGS = {
    "owner": "wasi-qollqui",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="wasi_qollqui_medallion_pipeline",
    description="Bronze CSV -> Silver Parquet -> Gold Parquet (Dataproc Serverless) + BQ External + BI tables",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 1),
    schedule=None,     # manual
    catchup=False,
    tags=["wasi-qollqui", "medallion", "dataproc", "bigquery"],
) as dag:

    submit_dataproc_batch = BashOperator(
        task_id="submit_dataproc_batch",
        bash_command=(
            "gcloud dataproc batches submit pyspark "
            "{{ params.pyspark_uri }} "
            "--project={{ params.project_id }} "
            "--region={{ params.region }} "
            "--deps-bucket=gs://{{ params.bucket }} "
            "--properties="
            "spark.wasi.bucket={{ params.bucket }},"
            "spark.wasi.dataset={{ params.dataset }},"
            "spark.sql.shuffle.partitions=4,"
            "spark.default.parallelism=4"
        ),
        params={
            "pyspark_uri": PYSPARK_URI,
            "project_id": PROJECT_ID,
            "region": REGION,
            "bucket": BUCKET,
            "dataset": DATASET,
        },
    )

    create_external_tables = BashOperator(
        task_id="create_external_tables",
        bash_command=(
            "bq --project_id={{ params.project_id }} mk "
            "--external_table_definition=PARQUET=gs://{{ params.bucket }}/gold/parquet/kpi_cliente/* "
            "{{ params.dataset }}.ext_kpi_cliente || true; "
            "bq --project_id={{ params.project_id }} mk "
            "--external_table_definition=PARQUET=gs://{{ params.bucket }}/gold/parquet/kpi_deuda_categoria/* "
            "{{ params.dataset }}.ext_kpi_deuda_categoria || true"
        ),
        params={
            "project_id": PROJECT_ID,
            "bucket": BUCKET,
            "dataset": DATASET,
        },
    )

    materialize_bi_tables = BashOperator(
        task_id="materialize_bi_tables",
        bash_command="""
            set -e

            bq --project_id=project-53930c9b-4e21-4329-b74 query --use_legacy_sql=false \
            'CREATE OR REPLACE TABLE `wasi_qollqui2.kpi_cliente_bi`
            AS SELECT * FROM `wasi_qollqui2.ext_kpi_cliente`';

            bq --project_id=project-53930c9b-4e21-4329-b74 query --use_legacy_sql=false \
            'CREATE OR REPLACE TABLE `wasi_qollqui2.kpi_deuda_categoria_bi`
            AS SELECT * FROM `wasi_qollqui2.ext_kpi_deuda_categoria`';
            """
    )

    submit_dataproc_batch >> create_external_tables >> materialize_bi_tables