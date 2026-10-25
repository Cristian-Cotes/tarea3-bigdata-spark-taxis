
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, min, max, hour, to_timestamp, sum as spark_sum
)

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Tarea3_BigData_Taxis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n========== PROCESAMIENTO BATCH ==========")

# 2. Cargar el archivo CSV
ruta = "/home/vboxuser/Tarea3_BigData/train.csv"

df = spark.read.csv(
    ruta,
    header=True,
    inferSchema=True
)

print("\n--- Esquema del conjunto de datos ---")
df.printSchema()

# 3. Mostrar cantidad de registros y columnas
print("\n--- Dimensiones del conjunto de datos ---")
print("Registros:", df.count())
print("Columnas:", len(df.columns))

# 4. Mostrar algunos registros
print("\n--- Primeros cinco registros ---")
df.show(5, truncate=False)

# 5. Revisar valores nulos
print("\n--- Valores nulos por columna ---")
df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show(truncate=False)

# 6. Eliminar registros duplicados y datos esenciales incompletos
df_limpio = df.dropDuplicates(["id"])

df_limpio = df_limpio.dropna(
    subset=[
        "id",
        "pickup_datetime",
        "trip_duration",
        "passenger_count"
    ]
)

# Mantener duraciones positivas y cantidad de pasajeros válida
df_limpio = df_limpio.filter(
    (col("trip_duration") > 0) &
    (col("passenger_count") > 0)
)

print("\n--- Resumen de limpieza ---")
print("Registros originales:", df.count())
print("Registros después de limpieza:", df_limpio.count())

# 7. Estadísticas de duración de viajes
print("\n--- Estadísticas de duración (segundos) ---")
df_limpio.select(
    avg("trip_duration").alias("duracion_promedio"),
    min("trip_duration").alias("duracion_minima"),
    max("trip_duration").alias("duracion_maxima")
).show(truncate=False)

# 8. Cantidad de viajes por hora de recogida
df_horas = df_limpio.withColumn(
    "fecha_recogida",
    to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "hora",
    hour(col("fecha_recogida"))
)

print("\n--- Viajes por hora de recogida ---")
resumen_horas = df_horas.groupBy("hora").agg(
    count("*").alias("cantidad_viajes"),
    avg("trip_duration").alias("duracion_promedio_segundos")
).orderBy("hora")

resumen_horas.show(24, truncate=False)

# 9. Guardar resultados
salida = "/home/vboxuser/Tarea3_BigData/resultados_batch"

resumen_horas.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(salida + "/viajes_por_hora")

df_limpio.select(
    "id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_duration"
).limit(1000).coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(salida + "/muestra_limpia")

print("\nResultados guardados en:", salida)
print("\n========== PROCESAMIENTO FINALIZADO ==========")

spark.stop()
