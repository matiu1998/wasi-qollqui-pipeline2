# src/pipeline_wasi_qollqui.py
# Wasi Qollqui - Spark (Local o Dataproc Serverless) - Medallion
# Bronze CSV -> Silver Parquet -> Gold Parquet
# Arquitectura: GCS (Parquet) + BigQuery External (workflow) + tabla BI materializada (workflow).

import os
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


# =========================
# Helpers de entorno
# =========================
def _get_env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = os.environ.get(k)
        if v:
            return v
    return default


def _abspath_posix(p: str) -> str:
    """
    Convierte a ruta absoluta con / (para file:/// en Windows).
    """
    return os.path.abspath(p).replace("\\", "/")


# =========================
# Mode (LOCAL vs GCS)
# =========================
MODE = os.environ.get("WASI_MODE", "gcs").strip().lower()  # "local" o "gcs"
if MODE not in ("local", "gcs"):
    raise ValueError("WASI_MODE debe ser 'local' o 'gcs'")

LOCAL_BASE = os.environ.get("WASI_LOCAL_BASE", ".").strip()

# En local: usamos file:///abs/path para evitar líos de Hadoop en Windows
if MODE == "local":
    base_abs = _abspath_posix(LOCAL_BASE)

    BRONZE = f"file:///{base_abs}/data"

    # --- salida fuera del workspace (recomendado en Windows) ---
    SILVER = "file:///C:/tmp_wasi/silver"
    GOLD   = "file:///C:/tmp_wasi/gold"
    WAREHOUSE_DIR = "file:///C:/tmp_wasi/warehouse"
else:
    BRONZE = ""
    SILVER = ""
    GOLD = ""
    WAREHOUSE_DIR = ""
    LOCAL_DIR = ""


# =========================
# Spark session
# =========================

# Defaults razonables para Windows local
DEFAULT_LOCAL_MASTER = os.environ.get("WASI_SPARK_MASTER", "local[2]")
DEFAULT_LOCALDIR_WIN = os.environ.get("WASI_SPARK_LOCAL_DIR", r"C:\temp_spark")

# En local, conviene que spark.local.dir sea RUTA LOCAL (NO file:///)
if MODE == "local":
    # si te pasan SPARK_LOCAL_DIRS / spark.local.dir por fuera, respétalo
    # si no, usa C:\temp_spark
    local_dir_from_env = os.environ.get("SPARK_LOCAL_DIR") or os.environ.get("SPARK_LOCAL_DIRS")
    if local_dir_from_env:
    # Si vienen varias, toma la primera
        local_dir_from_env = local_dir_from_env.split(";")[0].split(",")[0].strip()
    LOCAL_DIR = (local_dir_from_env or DEFAULT_LOCALDIR_WIN).strip()

    # crea carpeta si no existe
    try:
        os.makedirs(LOCAL_DIR, exist_ok=True)
    except Exception as e:
        print(f"[WARN] No se pudo crear LOCAL_DIR={LOCAL_DIR}: {e}")

builder = SparkSession.builder.appName("wasi-qollqui-medallion-parquet")

# --- IMPORTANTE ---
# No forzamos master si ya viene definido por fuera (PYSPARK_SUBMIT_ARGS / spark-submit)
# Solo ponemos un default si NO está definido.
if MODE == "local":
    # Heurística: si te pasaron algo por PYSPARK_SUBMIT_ARGS, no lo pises
    # (si quieres forzar desde aquí, setea WASI_SPARK_FORCE_MASTER=1)
    force_master = os.environ.get("WASI_SPARK_FORCE_MASTER", "0") == "1"
    submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    passed_master = ("--master" in submit_args) or ("spark.master" in submit_args)

    if force_master or (not passed_master):
        builder = builder.master(DEFAULT_LOCAL_MASTER)

# Configs anti-Windows pain (en Dataproc no molestan)
builder = (
    builder
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.io.nativeio.NativeIO", "false")
    .config("spark.hadoop.hadoop.native.lib", "false")
    .config("spark.hadoop.util.NativeCodeLoader", "false")
    .config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    )
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
    .config("spark.hadoop.fs.file.disable.cache", "true")
    .config("spark.hadoop.fs.file.checksum.enabled", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.file",
            "org.apache.hadoop.fs.FileSystem")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")        
)

# Ajustes específicos para local (sin pisar lo que venga por fuera)
if MODE == "local":
    # warehouse sí puede ser file:/// (es usado por Spark SQL)
    builder = builder.config("spark.sql.warehouse.dir", WAREHOUSE_DIR)

    # spark.local.dir DEBE ser path local normal
    # respeta spark.local.dir si ya viene por fuera, a menos que fuerces
    # Si ya viene por fuera, respétalo; si no, usamos LOCAL_DIR
    already_set_localdir = (
        ("--conf spark.local.dir=" in os.environ.get("PYSPARK_SUBMIT_ARGS", "")) or
        ("SPARK_LOCAL_DIRS" in os.environ) or
        ("SPARK_LOCAL_DIR" in os.environ)
    )

    if not already_set_localdir:
        builder = builder.config("spark.local.dir", LOCAL_DIR)

    # Reduce paralelismo por defecto (puedes sobreescribir por fuera)
    builder = (
        builder
        .config("spark.sql.shuffle.partitions", os.environ.get("WASI_SHUFFLE_PARTITIONS", "2"))
        .config("spark.default.parallelism", os.environ.get("WASI_DEFAULT_PARALLELISM", "2"))
        # Mejor traceback si el worker muere
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        # Para Windows a veces ayuda:
        .config("spark.python.worker.reuse", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

print("[DEBUG] spark.master =", spark.sparkContext.master)
print("[DEBUG] spark.local.dir =", spark.conf.get("spark.local.dir", ""))
print("[DEBUG] warehouse.dir =", spark.conf.get("spark.sql.warehouse.dir", ""))
print("[DEBUG] commitProtocol =", spark.conf.get("spark.sql.sources.commitProtocolClass", ""))

# =========================
# Configs GCS / BigQuery (solo para modo gcs)
# =========================
PROJECT_ID = spark.conf.get(
    "spark.wasi.project",
    _get_env_any(["GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "CLOUDSDK_CORE_PROJECT"], default="")
)
DATASET = spark.conf.get("spark.wasi.dataset", "wasi_qollqui")
BQ_LOCATION = spark.conf.get("spark.wasi.bq_location", "us-central1")

if MODE == "gcs":
    BUCKET = spark.conf.get("spark.wasi.bucket", "")
    if not BUCKET:
        raise ValueError("Falta spark.wasi.bucket. Pásalo en el job con --properties=spark.wasi.bucket=TU_BUCKET")

    BRONZE = f"gs://{BUCKET}/bronze"
    SILVER = f"gs://{BUCKET}/silver/parquet"
    GOLD = f"gs://{BUCKET}/gold/parquet"
else:
    BUCKET = ""


# =========================
# Utils
# =========================
def normalize_cols(df: DataFrame) -> DataFrame:
    for c in df.columns:
        new_c = c.strip().lower().replace(" ", "_")
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df


def safe_div(numer_col, denom_col):
    return F.when(denom_col.isNull() | (denom_col == 0), F.lit(None)).otherwise(numer_col / denom_col)


def to_date_multi(col_name: str) -> F.Column:
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "MM/dd/yyyy"),
        F.to_date(F.col(col_name))
    )


def write_parquet(df: DataFrame, path: str) -> None:
    """
    Escribe Parquet overwrite.
    En local, path ya viene como file:///... (muy importante).
    """
    df.write.mode("overwrite").parquet(path)


def _path_exists_gcs(gs_path: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(gs_path)
    fs = p.getFileSystem(hconf)
    return fs.exists(p)


def _require_exists(path: str) -> None:
    if MODE == "local":
        # convertir file:///C:/... a C:/... para exists()
        if path.startswith("file:///"):
            local_path = path.replace("file:///", "")
        else:
            local_path = path
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"[LOCAL] No existe el archivo: {local_path}")
    else:
        if not _path_exists_gcs(path):
            raise FileNotFoundError(f"[GCS] No existe el archivo: {path}")


def log_df(name: str, df: DataFrame, key_cols: Optional[List[str]] = None) -> None:
    cnt = df.count()
    print(f"[INFO] {name}: rows={cnt}, cols={len(df.columns)}")
    if key_cols:
        for k in key_cols:
            if k in df.columns:
                nulls = df.filter(F.col(k).isNull()).count()
                if nulls > 0:
                    print(f"[WARN] {name}: NULLs en {k} = {nulls}")


# =========================
# Reader Bronze
# =========================
def read_csv_bronze(name: str, schema: T.StructType) -> DataFrame:
    if MODE == "local":
        path = f"{BRONZE}/{name}.csv"  # BRONZE ya es file:///abs/data
    else:
        path = f"{BRONZE}/{name}.csv"

    _require_exists(path)

    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(path)
    )
    return normalize_cols(df)


# =========================
# Schemas (cabeceras reales)
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
print(f"[INFO] MODE={MODE}")
print(f"[INFO] BRONZE={BRONZE}")
print(f"[INFO] SILVER={SILVER}")
print(f"[INFO] GOLD={GOLD}")

clientes = read_csv_bronze("clientes", schemas["clientes"]).withColumn("fecha_registro", to_date_multi("fecha_registro"))
deuda = (read_csv_bronze("deuda", schemas["deuda"])
         .withColumn("fecha_vencimiento", to_date_multi("fecha_vencimiento"))
         .withColumn("estado", F.upper(F.trim(F.col("estado")))))
pagos = read_csv_bronze("pagos", schemas["pagos"]).withColumn("fecha_pago", to_date_multi("fecha_pago"))
gestiones = (read_csv_bronze("gestiones_cobranza", schemas["gestiones_cobranza"])
             .withColumn("fecha_gestion", to_date_multi("fecha_gestion"))
             .withColumn("resultado", F.upper(F.trim(F.col("resultado"))))
             .withColumn("canal", F.upper(F.trim(F.col("canal")))))
productos = (read_csv_bronze("productos", schemas["productos"])
             .withColumn("categoria", F.upper(F.trim(F.col("categoria"))))
             .withColumn("producto", F.trim(F.col("producto"))))
promesas = read_csv_bronze("promesas_pago", schemas["promesas_pago"]).withColumn("fecha_promesa", to_date_multi("fecha_promesa"))

# Calidad mínima
clientes = clientes.filter(F.col("customer_id").isNotNull())
deuda = deuda.filter(F.col("debt_id").isNotNull() & F.col("customer_id").isNotNull())
pagos = pagos.filter(F.col("payment_id").isNotNull() & F.col("customer_id").isNotNull())
gestiones = gestiones.filter(F.col("gestion_id").isNotNull() & F.col("customer_id").isNotNull())
productos = productos.filter(F.col("product_id").isNotNull())
promesas = promesas.filter(F.col("promesa_id").isNotNull() & F.col("customer_id").isNotNull())

# Logs
log_df("clientes_silver", clientes, ["customer_id"])
log_df("deuda_silver", deuda, ["debt_id", "customer_id"])
log_df("pagos_silver", pagos, ["payment_id", "customer_id"])
log_df("gestiones_silver", gestiones, ["gestion_id", "customer_id"])
log_df("productos_silver", productos, ["product_id"])
log_df("promesas_silver", promesas, ["promesa_id", "customer_id"])

# TEST escritura mínima (para verificar que Windows no rompe en commit)
print("[TEST] Writing tiny parquet (JVM-only)...")
(
    spark.range(1)  # <-- JVM-only, no python worker
    .selectExpr("cast(id as int) as id", "'ok' as msg")
    .write.mode("overwrite")
    .parquet(f"{SILVER}/__test")
)
print("[TEST] OK tiny parquet")

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
    F.sum("monto_deuda").alias("total_deuda"),
    F.countDistinct("debt_id").alias("num_deudas"),
    F.min("fecha_vencimiento").alias("min_fecha_vencimiento"),
    F.max("fecha_vencimiento").alias("max_fecha_vencimiento"),
)

pagos_cli = pagos.groupBy("customer_id").agg(
    F.sum("monto_pago").alias("total_pagado"),
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

log_df("kpi_cliente_gold", kpi_cliente, ["customer_id"])
write_parquet(kpi_cliente, f"{GOLD}/kpi_cliente")

deuda_prod = (
    deuda.join(productos, on="product_id", how="left")
    .groupBy("categoria")
    .agg(
        F.sum("monto_deuda").alias("total_deuda_categoria"),
        F.countDistinct("debt_id").alias("num_deudas_categoria"),
    )
)
log_df("kpi_deuda_categoria_gold", deuda_prod, ["categoria"])
write_parquet(deuda_prod, f"{GOLD}/kpi_deuda_categoria")

print("[OK] Silver Parquet escrito en:", SILVER)
print("[OK] Gold Parquet escrito en:", GOLD)

if MODE == "gcs":
    print("[NEXT] Workflow crea/actualiza External Tables en BigQuery apuntando a GOLD (Parquet).")
else:
    print("[LOCAL] No se escribe nada en BigQuery. Solo Parquet en ./out/")

spark.stop()