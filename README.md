# UNAD – Tarea 3: Procesamiento y análisis de datos con Big Data

## Descripción del proyecto

Este proyecto académico desarrolla un ejercicio de procesamiento y análisis de datos de sensores mediante herramientas del ecosistema Big Data. Se implementaron procesos de generación de datos, transmisión de eventos con Apache Kafka y análisis mediante Apache Spark y PySpark.

El trabajo integra dos modalidades de procesamiento: batch (por lotes) y streaming (en tiempo real), permitiendo explorar diferentes estrategias para obtener información a partir de registros de temperatura y humedad.

## Objetivo general

Implementar una solución de procesamiento de datos utilizando Apache Spark, PySpark y Apache Kafka para analizar registros de sensores y generar resultados estadísticos mediante procesamiento batch y streaming.

## Tecnologías utilizadas

- Ubuntu Server: entorno de ejecución.
- Python: desarrollo de scripts.
- Apache Kafka: transmisión de mensajes.
- Apache Spark y PySpark: procesamiento y análisis distribuido.
- GitHub: publicación y documentación del código.

## Estructura del proyecto

- `productor.py`: script de generación y envío de eventos a Kafka.
- `streaming.py`: procesamiento de datos recibidos mediante streaming.
- `batch.py`: lectura, limpieza y análisis de datos históricos.
- Archivos CSV: resultados obtenidos durante las ejecuciones.

## Desarrollo

### 1. Generación de datos

Se preparó un conjunto de datos históricos con 3.000 registros de tres sensores. Cada registro contiene un identificador, temperatura, humedad y marca de tiempo.

### 2. Procesamiento batch

Se utilizó PySpark para cargar el archivo CSV, examinar su estructura, eliminar valores nulos y duplicados y aplicar filtros de validación sobre las mediciones.

Posteriormente se agruparon los registros por sensor y se calcularon el número de observaciones, la temperatura promedio, mínima y máxima, así como la humedad promedio.

### 3. Procesamiento streaming

Se implementó un productor de eventos con Apache Kafka y un proceso de lectura mediante Spark Structured Streaming. Los eventos se agrupan por sensor para calcular indicadores de temperatura y humedad, y se generan archivos CSV como evidencia del procesamiento.

## Resultados del análisis batch

El conjunto de datos contenía 3.000 registros y conservó la misma cantidad después de las operaciones de limpieza.

| Sensor | Registros | Temperatura promedio | Temperatura mínima | Temperatura máxima | Humedad promedio |
|---|---:|---:|---:|---:|---:|
| sensor_01 | 1000 | 28.36 | 18.00 | 38.00 | 56.96 |
| sensor_02 | 1000 | 27.72 | 18.00 | 37.99 | 58.28 |
| sensor_03 | 1000 | 27.98 | 18.01 | 38.00 | 57.41 |

Los resultados muestran valores promedio de temperatura similares entre los tres sensores. El sensor_01 presentó el promedio de temperatura más alto, mientras que el sensor_02 registró el promedio de humedad más elevado.

## Resultados del procesamiento streaming

Durante la ejecución se comprobó la recepción de eventos desde Kafka mediante Spark Structured Streaming. Se generaron resultados agrupados por sensor y se exportaron archivos CSV con los campos de identificador del sensor, cantidad de registros, temperatura promedio y humedad promedio.

## Conclusiones

La práctica permitió integrar herramientas de procesamiento de datos utilizadas en entornos Big Data. El análisis batch facilitó el tratamiento de un conjunto histórico de observaciones y la generación de indicadores estadísticos por sensor.

Por otra parte, el procesamiento streaming permitió trabajar con un flujo de eventos y observar cómo Spark procesa información recibida desde Kafka.

La combinación de ambas modalidades demuestra la utilidad de utilizar diferentes enfoques de procesamiento según las características de los datos y las necesidades de análisis.

## Contexto académico

Universidad Nacional Abierta y a Distancia (UNAD).

Actividad: Tarea 3 – Aplicación del Business Intelligence.

Proyecto desarrollado como ejercicio práctico individual de procesamiento y análisis de datos.
