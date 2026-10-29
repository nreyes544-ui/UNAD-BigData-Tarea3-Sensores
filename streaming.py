from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (SparkSession.builder.appName('UNAD_Tarea3_Sensores')
         .master('local[2]').config('spark.sql.shuffle.partitions', '2').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
schema = StructType([
    StructField('sensor_id', StringType()),
    StructField('temperatura', DoubleType()),
    StructField('humedad', DoubleType()),
    StructField('timestamp', StringType()),
])
raw = (spark.readStream.format('kafka')
       .option('kafka.bootstrap.servers', '127.0.0.1:9092')
       .option('subscribe', 'sensor_data')
       .option('startingOffsets', 'earliest').load())
sensores = raw.select(from_json(col('value').cast('string'), schema).alias('dato')).select('dato.*')
resumen = sensores.groupBy('sensor_id').agg(count('*').alias('registros'),
    spark_round(avg('temperatura'), 2).alias('temperatura_promedio'),
    spark_round(avg('humedad'), 2).alias('humedad_promedio'))

def guardar_resultados(batch_df, batch_id):
    destino = str(Path.home() / "bigdata_tarea3" / "evidencias" / "resultados_csv")
    (batch_df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(destino))
    print(f"Resultados CSV actualizados - lote {batch_id}", flush=True)

query = (resumen.writeStream.outputMode('complete').foreachBatch(guardar_resultados)
         .option('truncate', 'false').trigger(processingTime='10 seconds').start())
print('Spark Streaming escuchando sensor_data; Ctrl+C para detener.', flush=True)
query.awaitTermination()

# Exportacion de resultados a CSV
