# 🍃 Análisis Avanzado de Calidad del Aire en Colombia (Batch Processing con PySpark & HDFS)

Este repositorio contiene la solución analítica por lotes (*Batch Processing*) desarrollada sobre el conjunto de datos abierto de **Calidad del Aire en Colombia (Promedio Anual)** proveniente del **IDEAM / SISAIRE** y publicado en [datos.gov.co](https://www.datos.gov.co/).

La solución utiliza **Apache Spark (PySpark)** sobre el sistema de archivos distribuido **Hadoop HDFS** para realizar ingesta, sanitización de datos, estadísticas descriptivas distribuidas, transformaciones con funciones de ventana (*Window Functions*) e inferencia de tendencias históricas de contaminación atmosférica.

---

## 📐 Arquitectura del Sistema

```
 ┌─────────────────────────────────────────┐
 │   Datos Abiertos Colombia (IDEAM/SISAIRE)│
 └────────────────────┬────────────────────┘
                      │ Local CSV Upload
                      ▼
 ┌─────────────────────────────────────────┐
 │       Hadoop HDFS Data Node             │
 │ (hdfs://localhost:9000/Proyecto...)    │
 └────────────────────┬────────────────────┘
                      │ Ingesta Distribuida
                      ▼
 ┌─────────────────────────────────────────┐
 │        PySpark Batch Application        │
 │  ┌───────────────────────────────────┐  │
 │  │ Pipeline 1: Clean & Casting       │  │
 │  ├───────────────────────────────────┤  │
 │  │ Pipeline 2: PM2.5/PM10 Stats      │  │
 │  ├───────────────────────────────────┤  │
 │  │ Pipeline 3: Window Ranking (Top 1)│  │
 │  ├───────────────────────────────────┤  │
 │  │ Pipeline 4: Historical Trend      │  │
 │  └───────────────────────────────────┘  │
 └────────────────────┬────────────────────┘
                      │ Output / Reporting
                      ▼
 ┌─────────────────────────────────────────┐
 │      Consola Web & Spark UI (:4040)      │
 └─────────────────────────────────────────┘
```

---

## 🛠️ Tecnologías Utilizadas

* **Lenguaje:** Python 3.10+
* **Motor de Cómputo Distribuido:** Apache Spark 3.5.x (PySpark)
* **Almacenamiento Distribuido:** Apache Hadoop HDFS 3.x
* **Infraestructura de SO:** Ubuntu Server 22.04 LTS (VirtualBox / SSH)

---

## 🧩 Estructura de Pipelines Analíticos

El script `tarea3_calidad_aire.py` está modularizado en 4 pipelines secuenciales optimizados:

### 1. Pipeline 1: Ingesta, Estructuración y Normalización (HDFS)
* **Conexión:** Lectura del archivo `.csv` desde HDFS (`hdfs://localhost:9000/ProyectoCalidadAire/calidad_aire.csv`).
* **Sanitización:** Tratamiento de cadenas con separadores numéricos de miles e internacionales mediante `F.regexp_replace` para corregir la conversión a tipos numéricos estricto (`integer` y `double`).
* **Caché:** Persistencia temporal en RAM (`df_clean.cache()`) para optimizar el grafo de ejecuciones posteriores (DAG).

### 2. Pipeline 2: Análisis Estadístico de Material Particulado ($PM_{2.5}$ y $PM_{10}$)
* **Filtrado:** Selección exclusiva de los contaminantes más nocivos para la salud humana.
* **Métricas:** Cálculo en paralelo de medidas de tendencia central y dispersión: conteo, media aritmética, desviación estándar, mediana ($P_{50}$), percentil crítico ($P_{95}$) y máximo valor registrado.

### 3. Pipeline 3: Ranking de Municipios Críticos (Window Functions)
* **Particionamiento por Ventana:** Implementación de `Window.partitionBy("departamento", "contaminante").orderBy(...)` para aislar el comportamiento intrínseco de cada territorio.
* **Ranking:** Asignación de rangos (`F.rank()`) para extraer los municipios con mayor cantidad acumulada de días en sobrepaso de los límites normativos (**Resolución 2254 de 2017**).

### 4. Pipeline 4: Evaluación de Tendencia Temporal e Historial de $PM_{2.5}$
* **Serie Temporal:** Agregación cronológica interanual de la concentración promedio nacional de $PM_{2.5}$.
* **Impacto Ambiental:** Acumulación del volumen global de días críticos registrados por el país.

---

## 🚀 Requisitos y Guía de Despliegue

### Prerrequisitos
1. Tener iniciado el clúster de **Hadoop HDFS** (`start-all.sh`).
2. Tener instalado **PySpark** en el entorno local o virtual (`pip install pyspark`).

---

### Paso 1: Carga de Datos a HDFS

1. Conéctate por SSH a la VM con el usuario `hadoop`:
   ```bash
   # Crear el directorio del proyecto en HDFS
   hdfs dfs -mkdir -p /ProyectoCalidadAire/

   # Subir el archivo local descargado a HDFS
   hdfs dfs -put /home/hadoop/calidad_aire.csv /ProyectoCalidadAire/

   # Verificar la correcta carga del archivo
   hdfs dfs -ls /ProyectoCalidadAire/
   ```

---

### Paso 2: Ejecución del Script Batch

Conéctate en una terminal con el usuario ejecutante (`vboxuser`) e invoca la aplicación PySpark:

```bash
# Opción A: Ejecución directa con intérprete Python
python3 tarea3_calidad_aire.py

# Opción B: Envío mediante Spark Submit
spark-submit --master local[4] tarea3_calidad_aire.py
```

---

### Paso 3: Monitoreo en Tiempo Real (Spark Web UI)

Mientras la aplicación esté procesando las tareas, abre tu navegador web preferido e ingresa a:

```text
http://<IP_DE_TU_VM>:4040
```

Pestañas destacadas para evaluar durante la sustentación:
* **Jobs:** Inspección de etapas completadas (*Jobs* 1 al 4).
* **SQL/DataFrame:** Visualización del Plan Físico y el Grafo Acíclico Dirigido (DAG) generado por el motor **Catalyst**.

---

## 📊 Ejemplo de Resultados Esperados

### Estadísticas de Material Particulado (Pipeline 2)
```text
+------------+--------------+------------+-------------+-----------+-----------+--------------+
|contaminante|num_mediciones|media_conctr|desv_estandar|mediana_p50|p95_critico|max_registrado|
+------------+--------------+------------+-------------+-----------+-----------+--------------+
|        PM10|          5706|        37.2|        15.55|      34.63|       66.1|         162.3|
|       PM2.5|          3711|       16.84|         7.04|       15.6|       29.4|          68.6|
+------------+--------------+------------+-------------+-----------+-----------+--------------+
```

### Tendencia Histórica de $PM_{2.5}$ (Pipeline 4)
```text
+----+----------------------+------------------------+
|anio|promedio_nacional_pm25|total_dias_criticos_pais|
+----+----------------------+------------------------+
|2018|                 16.53|                  6562.0|
|2019|                 18.51|                 11988.0|
|2020|                 16.98|                 21614.0|
|2021|                 15.49|                 16974.0|
|2022|                 15.39|                 17968.0|
|2023|                 14.00|                  8118.0|
|2024|                 14.73|                  3733.0|
|2025|                 12.14|                  2374.0|
+----+----------------------+------------------------+
```

---

## 📄 Licencia y Fuentes de Datos

* **Fuente original:** Instituto de Hidrología, Meteorología y Estudios Ambientales (IDEAM) / Sistema de Información sobre Calidad del Aire (SISAIRE).
* **Licencia de datos:** Datos Abiertos Gobierno de Colombia.
