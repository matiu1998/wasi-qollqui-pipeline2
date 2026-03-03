from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("check").getOrCreate()

silver = "file:///C:/tmp_wasi/silver"
gold   = "file:///C:/tmp_wasi/gold"

print("kpi_cliente rows =", spark.read.parquet(f"{gold}/kpi_cliente").count())
print("kpi_deuda_categoria rows =", spark.read.parquet(f"{gold}/kpi_deuda_categoria").count())

spark.stop()