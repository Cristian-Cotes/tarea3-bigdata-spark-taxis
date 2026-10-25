import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

# Carpeta donde Spark guardó los resultados
carpeta = "/home/vboxuser/Tarea3_BigData/resultados_batch/viajes_por_hora"

# Buscar el archivo CSV generado por Spark
archivos = glob.glob(os.path.join(carpeta, "part-*.csv"))

if not archivos:
    print("ERROR: No se encontró el archivo CSV de resultados.")
    print("Verifica que el procesamiento Batch haya finalizado correctamente.")
    raise SystemExit(1)

# Leer los resultados
df = pd.read_csv(archivos[0])

# Ordenar los resultados por hora
df = df.sort_values("hora")

# Crear carpeta para guardar las gráficas
salida = "/home/vboxuser/Tarea3_BigData/graficas"
os.makedirs(salida, exist_ok=True)

# Gráfica 1: cantidad de viajes por hora
plt.figure(figsize=(10, 6))
plt.bar(df["hora"], df["cantidad_viajes"])
plt.title("Cantidad de viajes en taxi por hora")
plt.xlabel("Hora de recogida")
plt.ylabel("Cantidad de viajes")
plt.xticks(range(24))
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig(os.path.join(salida, "viajes_por_hora.png"), dpi=150)
plt.close()

# Gráfica 2: duración promedio de los viajes por hora
plt.figure(figsize=(10, 6))
plt.plot(
    df["hora"],
    df["duracion_promedio_segundos"],
    marker="o"
)
plt.title("Duración promedio de los viajes por hora")
plt.xlabel("Hora de recogida")
plt.ylabel("Duración promedio (segundos)")
plt.xticks(range(24))
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig(
    os.path.join(salida, "duracion_promedio_por_hora.png"),
    dpi=150
)
plt.close()

print("Gráficas generadas correctamente.")
print("Ubicación:", salida)
print("Archivos:")
print("- viajes_por_hora.png")
print("- duracion_promedio_por_hora.png")

