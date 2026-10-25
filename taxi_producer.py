
import csv
import json
import time
from kafka import KafkaProducer

ARCHIVO = "/home/vboxuser/Tarea3_BigData/train.csv"
TOPIC = "taxi_data"

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

try:
    with open(ARCHIVO, "r", encoding="utf-8") as archivo:
        lector = csv.DictReader(archivo)

        for fila in lector:
            evento = {
                "id": fila["id"],
                "pickup_datetime": fila["pickup_datetime"],
                "passenger_count": int(fila["passenger_count"] or 0),
                "trip_duration": int(fila["trip_duration"] or 0)
            }

            producer.send(TOPIC, evento)
            print("Enviado:", evento)
            time.sleep(1)

except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")

finally:
    producer.flush()
    producer.close()
