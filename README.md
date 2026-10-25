# Procesamiento Batch y análisis de viajes en taxi con Apache Spark

## 1. Descripción del proyecto

Este proyecto desarrolla un proceso de análisis de datos utilizando Apache Spark y Python. Se utiliza un conjunto de datos de viajes en taxi que contiene información como el identificador del viaje, la fecha y hora de recogida, la cantidad de pasajeros y la duración del recorrido.

El objetivo es aplicar técnicas de procesamiento Batch para cargar, limpiar, transformar y analizar los datos, con el fin de obtener información que facilite la comprensión del comportamiento de los viajes durante las diferentes horas del día.

El proyecto también incluye la generación de gráficas mediante Matplotlib y Pandas para representar visualmente los resultados obtenidos.

## 2. Objetivos

### Objetivo general

Implementar un proceso de análisis de datos por lotes mediante Apache Spark para limpiar, transformar y analizar información de viajes en taxi, generando resultados estadísticos y visualizaciones que faciliten su interpretación.

### Objetivos específicos

- Cargar el conjunto de datos CSV mediante un DataFrame de Spark.
- Identificar valores nulos y revisar la estructura de los datos.
- Eliminar registros duplicados y registros incompletos en campos esenciales.
- Filtrar viajes con duraciones y cantidades de pasajeros no válidas.
- Calcular estadísticas descriptivas sobre la duración de los viajes.
- Agrupar los viajes por hora de recogida.
- Guardar los resultados del procesamiento en archivos CSV.
- Generar gráficas para facilitar la interpretación de los resultados.

## 3. Tecnologías utilizadas

- Python 3.
- Apache Spark 3.5.8.
- PySpark.
- Pandas.
- Matplotlib.
- Ubuntu Linux.
- Git y GitHub.

## 4. Estructura del proyecto

- `tarea3_batch.py`: ejecuta el procesamiento Batch, la limpieza, las transformaciones y el análisis estadístico.
- `graficas_batch.py`: lee los resultados del procesamiento Batch y genera las gráficas.
- `taxi_producer.py`: genera y envía registros de viajes al tópico de Kafka, si se incluye en el repositorio.
- `taxi_streaming.py`: procesa los mensajes recibidos desde Kafka mediante Spark Structured Streaming, si se incluye en el repositorio.
- `README.md`: contiene la documentación del proyecto.
- `.gitignore`: excluye archivos de datos y resultados que no es necesario versionar.

## 5. Preparación del entorno

Se requiere tener Python 3, Apache Spark y un entorno Linux configurado.

Para instalar las librerías utilizadas en la generación de gráficas, se pueden ejecutar los siguientes comandos:

```bash
python3 -m pip install --user pandas
python3 -m pip install --user matplotlib
```

El programa Batch utiliza PySpark, que viene incluido con la instalación de Spark empleada en el proyecto.

## 6. Preparación de los datos

El archivo de entrada se denomina `train.csv` y debe ubicarse en la siguiente ruta:

```text
/home/vboxuser/Tarea3_BigData/train.csv
```

El archivo de datos no se incluye en este repositorio debido a su tamaño. Para ejecutar el programa, es necesario disponer del conjunto de datos y ajustar la ruta en el código si se utiliza una ubicación diferente.

## 7. Ejecución del procesamiento Batch

Desde la terminal, ejecutar:

```bash
cd /home/vboxuser/Tarea3_BigData
```

Después, iniciar el procesamiento con:

```bash
/opt/spark/bin/spark-submit tarea3_batch.py
```

El programa realiza las siguientes operaciones:

1. Carga el archivo CSV en un DataFrame de Spark.
2. Muestra el esquema y las dimensiones del conjunto de datos.
3. Presenta una muestra de los registros.
4. Calcula los valores nulos por columna.
5. Elimina registros duplicados utilizando el identificador del viaje.
6. Elimina registros con datos esenciales incompletos.
7. Filtra viajes con duración y cantidad de pasajeros mayores que cero.
8. Calcula la duración promedio, mínima y máxima de los viajes.
9. Agrupa los registros por hora de recogida.
10. Guarda los resultados en archivos CSV.

## 8. Generación de gráficas

Después de completar el procesamiento Batch, ejecutar:

```bash
python3 graficas_batch.py
```

Las gráficas se guardan en:

```text
/home/vboxuser/Tarea3_BigData/graficas/
```

Los archivos generados son:

- `viajes_por_hora.png`: representa la cantidad de viajes por hora.
- `duracion_promedio_por_hora.png`: representa la duración promedio de los viajes por hora.

## 9. Resultados esperados

El proceso permite obtener:

- Una descripción de la estructura y las dimensiones del conjunto de datos.
- Un conteo de valores nulos por columna.
- Un conjunto de datos depurado mediante reglas de limpieza.
- Estadísticas descriptivas sobre la duración de los viajes.
- Una tabla con la cantidad de viajes y la duración promedio agrupadas por hora.
- Archivos CSV con los resultados del análisis.
- Dos gráficas para apoyar la interpretación de los datos.

Los resultados numéricos definitivos deben obtenerse al ejecutar el programa con el conjunto de datos completo.

## 10. Dificultades encontradas y soluciones implementadas

### Dificultad 1. Procesamiento de un conjunto de datos de gran tamaño

El archivo de entrada ocupa aproximadamente 192 MB, por lo que se requiere una herramienta adecuada para procesar los registros.

**Solución implementada:** se utilizó Apache Spark y sus DataFrames para cargar, transformar, filtrar y agrupar los datos.

### Dificultad 2. Calidad de los datos

Los registros pueden contener valores nulos, identificadores repetidos o valores no válidos para las variables analizadas.

**Solución implementada:** se incorporaron operaciones de eliminación de duplicados, descarte de registros incompletos en campos esenciales y filtros para conservar duraciones y cantidades de pasajeros positivas.

### Dificultad 3. Interpretación de los resultados

Los resultados estadísticos presentados únicamente en la terminal pueden dificultar la identificación de patrones.

**Solución implementada:** se utilizaron Pandas y Matplotlib para leer los resultados agrupados y generar gráficas de cantidad de viajes y duración promedio por hora.

### Dificultad 4. Instalación de librerías en la máquina virtual

Durante la preparación del entorno se presentaron dificultades para descargar algunas dependencias de Python.

**Solución implementada:** se aumentó el tiempo de espera y el número de reintentos de `pip`, y se verificó la instalación de las librerías mediante la consulta de sus versiones.

### Dificultad 5. Separación entre código y datos

El conjunto de datos ocupa un espacio considerable y no es necesario publicarlo junto con el código fuente.

**Solución implementada:** se documentó la ubicación esperada del archivo CSV y se excluyeron los datos y resultados generados del repositorio mediante `.gitignore`.

## 11. Conclusiones

El proyecto permitió aplicar conceptos de procesamiento de datos a gran escala utilizando Apache Spark. Se implementaron operaciones de carga, limpieza, transformación, agregación y análisis estadístico sobre un conjunto de datos de viajes en taxi.

La generación de gráficas facilitó la representación visual de los resultados agrupados por hora. Además, la documentación y el uso de GitHub permiten organizar el código fuente, registrar los cambios y facilitar su revisión y reutilización.

Para reproducir los resultados es necesario disponer del conjunto de datos original, configurar las dependencias y ejecutar los programas siguiendo las instrucciones descritas en este documento.
