
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = SparkSession.builder \
    .appName("TaxiStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("id", StringType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_duration", IntegerType(), True)
])

datos = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi_data") \
    .option("startingOffsets", "latest") \
    .load()

viajes = datos.select(
    from_json(
        col("value").cast("string"), esquema
    ).alias("viaje")
).select("viaje.*")

resumen = viajes.agg(
    count("*").alias("total_viajes"),
    avg("trip_duration").alias("duracion_promedio_segundos")
)

consulta = resumen.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

consulta.awaitTermination()
