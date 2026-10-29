from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, count, round as spark_round
)

spark = SparkSession.builder \
    .appName("UNAD_BigData_Analisis_Batch") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ruta = "/home/vboxuser/bigdata_tarea3/datos/sensores_historicos.csv"

df = spark.read.csv(
    ruta,
    header=True,
    inferSchema=True
)

print("\n=== DATOS ORIGINALES ===")
df.show(10)

print("\n=== ESTRUCTURA DEL DATASET ===")
df.printSchema()

print("\n=== TOTAL DE REGISTROS ===")
print(df.count())

df_limpio = df.dropna().dropDuplicates()

df_limpio = df_limpio.filter(
    (col("temperatura").between(0, 50)) &
    (col("humedad").between(0, 100))
)

print("\n=== REGISTROS DESPUES DE LIMPIEZA ===")
print(df_limpio.count())

resumen = df_limpio.groupBy("sensor_id").agg(
    count("*").alias("registros"),
    spark_round(avg("temperatura"), 2).alias("temperatura_promedio"),
    spark_round(min("temperatura"), 2).alias("temperatura_minima"),
    spark_round(max("temperatura"), 2).alias("temperatura_maxima"),
    spark_round(avg("humedad"), 2).alias("humedad_promedio")
)

print("\n=== RESULTADOS DEL ANALISIS BATCH ===")
resumen.orderBy("sensor_id").show(truncate=False)

resumen.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/home/vboxuser/bigdata_tarea3/evidencias/batch_resultados")

print("\nAnalisis batch completado correctamente.")

spark.stop()
