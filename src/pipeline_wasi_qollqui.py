# src/pipeline_wasi_qollqui.py
# Wasi Qollqui - Spark Serverless (Dataproc) - Medallion (Bronze CSV -> Silver Parquet -> Gold Parquet)
# Arquitectura: GCS (Parquet) + BigQuery External (creado en workflow) + tabla BI materializada (workflow).
# Compatible con Dataproc Serverless (Spark) sin Delta.

import os
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


# =========================
# Spark session
# =========================
spark = (
    SparkSession.builder
    .appName("wasi-qollqui-medallion-parquet")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("INFO")


# =========================
# Config (Spark conf / env)
# =========================
def _get_env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = os.environ.get(k)
        if v:
            return v
    return default


BUCKET = spark.conf.get("spark.wasi.bucket", "")
if not BUCKET:
    raise ValueError("Falta spark.wasi.bucket. Pásalo en el job con --properties=spark.wasi.bucket=TU_BUCKET")

PROJECT_ID = spark.conf.get(
    "spark.wasi.project",
    _get_env_any(["GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT"], default="")
)
DATASET = spark.conf.get("spark.wasi.dataset", "wasi_qollqui")
BQ_LOCATION = spark.conf.get("spark.wasi.bq_location", "us-central1")

# Paths Medallion
BRONZE = f"gs://{BUCKET}/bronze/csv"
SILVER = f"gs://{BUCKET}/silver/parquet"
GOLD = f"gs://{BUCKET}/gold/parquet"

# Tablas (nombres de archivo)
TABLES = ["clientes", "deuda", "pagos", "gestiones_cobranza", "productos", "promesas_pago"]


# =========================
# Utils
# =========================
def normalize_cols(df: DataFrame) -> DataFrame:
    """Normaliza columnas: strip, lowercase, espacios -> _"""
    for c in df.columns:
        new_c = c.strip().lower().replace(" ", "_")
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df


def read_csv_bronze(name: str, schema: T.StructType) -> DataFrame:
    """Lee CSV desde Bronze en GCS."""
    path = f"{BRONZE}/{name}.csv"
    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(path)
    )
    return normalize_cols(df)


def write_parquet(df: DataFrame, path: str) -> None:
    """Escribe Parquet overwrite."""
    (
        df.write
        .mode("overwrite")
        .parquet(path)
    )


def safe_div(numer_col, denom_col):
    """División segura (evita /0)."""
    return F.when(denom_col.isNull() | (denom_col == 0), F.lit(None)).otherwise(numer_col / denom_col)


def to_date_multi(col_name: str) -> F.Column:
    """
    Intenta parsear fecha con varios formatos comunes.
    Ajusta si tu data usa formatos distintos.
    """
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "MM/dd/yyyy"),
        F.to_date(F.col(col_name))  # fallback
    )


# =========================
# Schemas (estables)
# =========================
schemas: Dict[str, T.StructType] = {
    "clientes": T.StructType([
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("dni", T.StringType(), True),
        T.StructField("nombre", T.StringType(), True),
        T.StructField("telefono", T.StringType(), True),
        T.StructField("direccion", T.StringType(), True),
        T.StructField("fecha_registro", T.StringType(), True),
    ]),
    "deuda": T.StructType([
        T.StructField("debt_id", T.LongType(), True),
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("product_id", T.LongType(), True),
        T.StructField("monto_deuda", T.DoubleType(), True),
        T.StructField("fecha_vencimiento", T.StringType(), True),
        T.StructField("estado", T.StringType(), True),
    ]),
    "pagos": T.StructType([
        T.StructField("payment_id", T.LongType(), True),
        T.StructField("debt_id", T.LongType(), True),
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("monto_pago", T.DoubleType(), True),
        T.StructField("fecha_pago", T.StringType(), True),
    ]),
    "gestiones_cobranza": T.StructType([
        T.StructField("gestion_id", T.LongType(), True),
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("resultado", T.StringType(), True),
        T.StructField("canal", T.StringType(), True),
        T.StructField("fecha_gestion", T.StringType(), True),
    ]),
    "productos": T.StructType([
        T.StructField("product_id", T.LongType(), True),
        T.StructField("producto", T.StringType(), True),
        T.StructField("categoria", T.StringType(), True),
    ]),
    "promesas_pago": T.StructType([
        T.StructField("promesa_id", T.LongType(), True),
        T.StructField("customer_id", T.LongType(), True),
        T.StructField("debt_id", T.LongType(), True),
        T.StructField("monto_prometido", T.DoubleType(), True),
        T.StructField("fecha_promesa", T.StringType(), True),
    ]),
}


# =========================
# 1) Bronze -> Silver
# =========================
clientes = read_csv_bronze("clientes", schemas["clientes"]) \
    .withColumn("fecha_registro", to_date_multi("fecha_registro"))

deuda = read_csv_bronze("deuda", schemas["deuda"]) \
    .withColumn("fecha_vencimiento", to_date_multi("fecha_vencimiento")) \
    .withColumn("estado", F.upper(F.trim(F.col("estado"))))

pagos = read_csv_bronze("pagos", schemas["pagos"]) \
    .withColumn("fecha_pago", to_date_multi("fecha_pago"))

gestiones = read_csv_bronze("gestiones_cobranza", schemas["gestiones_cobranza"]) \
    .withColumn("fecha_gestion", to_date_multi("fecha_gestion")) \
    .withColumn("resultado", F.upper(F.trim(F.col("resultado")))) \
    .withColumn("canal", F.upper(F.trim(F.col("canal"))))

productos = read_csv_bronze("productos", schemas["productos"]) \
    .withColumn("categoria", F.upper(F.trim(F.col("categoria")))) \
    .withColumn("producto", F.trim(F.col("producto")))

promesas = read_csv_bronze("promesas_pago", schemas["promesas_pago"]) \
    .withColumn("fecha_promesa", to_date_multi("fecha_promesa"))

# Calidad mínima
clientes = clientes.filter(F.col("customer_id").isNotNull())
deuda = deuda.filter(F.col("debt_id").isNotNull() & F.col("customer_id").isNotNull())
pagos = pagos.filter(F.col("payment_id").isNotNull() & F.col("customer_id").isNotNull())
gestiones = gestiones.filter(F.col("gestion_id").isNotNull() & F.col("customer_id").isNotNull())
productos = productos.filter(F.col("product_id").isNotNull())
promesas = promesas.filter(F.col("promesa_id").isNotNull() & F.col("customer_id").isNotNull())

# Write Silver
write_parquet(clientes, f"{SILVER}/clientes")
write_parquet(deuda, f"{SILVER}/deuda")
write_parquet(pagos, f"{SILVER}/pagos")
write_parquet(gestiones, f"{SILVER}/gestiones_cobranza")
write_parquet(productos, f"{SILVER}/productos")
write_parquet(promesas, f"{SILVER}/promesas_pago")


# =========================
# 2) Silver -> Gold (KPIs)
# =========================
deuda_cli = deuda.groupBy("customer_id").agg(
    F.sum(F.col("monto_deuda")).alias("total_deuda"),
    F.countDistinct("debt_id").alias("num_deudas"),
    F.min("fecha_vencimiento").alias("min_fecha_vencimiento"),
    F.max("fecha_vencimiento").alias("max_fecha_vencimiento"),
)

pagos_cli = pagos.groupBy("customer_id").agg(
    F.sum(F.col("monto_pago")).alias("total_pagado"),
    F.countDistinct("payment_id").alias("num_pagos"),
    F.min("fecha_pago").alias("min_fecha_pago"),
    F.max("fecha_pago").alias("max_fecha_pago"),
)

kpi_cliente = (
    clientes.select("customer_id", "dni", "nombre")
    .join(deuda_cli, on="customer_id", how="left")
    .join(pagos_cli, on="customer_id", how="left")
    .withColumn("total_deuda", F.coalesce(F.col("total_deuda"), F.lit(0.0)))
    .withColumn("total_pagado", F.coalesce(F.col("total_pagado"), F.lit(0.0)))
    .withColumn("deuda_pendiente", (F.col("total_deuda") - F.col("total_pagado")))
    .withColumn("tasa_recuperacion", safe_div(F.col("total_pagado"), F.col("total_deuda")))
)

gest_cli = gestiones.groupBy("customer_id").agg(
    F.count("*").alias("num_gestiones"),
    F.countDistinct("canal").alias("num_canales"),
    F.max("fecha_gestion").alias("ultima_gestion"),
)

kpi_cliente = (
    kpi_cliente
    .join(gest_cli, on="customer_id", how="left")
    .withColumn("num_gestiones", F.coalesce(F.col("num_gestiones"), F.lit(0)))
    .withColumn("num_canales", F.coalesce(F.col("num_canales"), F.lit(0)))
)

# Write Gold
write_parquet(kpi_cliente, f"{GOLD}/kpi_cliente")

deuda_prod = (
    deuda.join(productos, on="product_id", how="left")
    .groupBy("categoria")
    .agg(
        F.sum("monto_deuda").alias("total_deuda_categoria"),
        F.countDistinct("debt_id").alias("num_deudas_categoria"),
    )
)
write_parquet(deuda_prod, f"{GOLD}/kpi_deuda_categoria")

print("OK: Silver Parquet escrito en:", SILVER)
print("OK: Gold Parquet escrito en:", GOLD)
print("Siguiente paso: el workflow crea/actualiza External Tables en BigQuery apuntando a GOLD (Parquet).")

spark.stop()