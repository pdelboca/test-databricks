# Databricks notebook source
# MAGIC %md 
# MAGIC # Pipeline ETL de Ejemplo
# MAGIC 
# MAGIC En el siguiente ejemplo repasaremos una tarea típica de ETL y su automatizacion.
# MAGIC 
# MAGIC <img src="https://static1.squarespace.com/static/52d1b75de4b0ed895b7e7de9/t/5aca8fdf2b6a289d08be9a8f/1523224551556/DataLakeZones_MelissaCoates.jpg?format=1000w"></img>

# COMMAND ----------

# MAGIC %md ## Datos de ejemplo
# MAGIC Tenemos logs de eventos en `/raw_data/structured-streaming/`

# COMMAND ----------

# MAGIC %fs ls /mnt/azure-dls/raw_data/structured-streaming/

# COMMAND ----------

# MAGIC %md Son aproximadamente 50 JSONs, tipicamente las aplicaciones escriben 1 log por día.

# COMMAND ----------

# MAGIC %fs head /mnt/azure-dls/raw_data/structured-streaming/file-0.json

# COMMAND ----------

# MAGIC %md 
# MAGIC Cada linea del archivo json contiene 2 campos: `time` and `action`.

# COMMAND ----------

# MAGIC %md ## Procesamiento y Análisis
# MAGIC El camino usual a recorrer es analizar los datos y, luego de obtenerse las métricas deseadas, guardarlo en alguna fuente de datos para que sea fácilmente accesible a los analistas.

# COMMAND ----------

from pyspark.sql.types import *

inputPath = "/mnt/azure-dls/raw_data/structured-streaming/"

# Since we know the data format already, let's define the schema to speed up processing (no need for Spark to infer schema)
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

# Static DataFrame representing data in the JSON files
staticInputDF = (
  spark
    .read
    .schema(jsonSchema)
    .json(inputPath)
)

display(staticInputDF)

# COMMAND ----------

staticInputDF. \
  repartition(1).\
  write.\
  format("com.databricks.spark.csv").\
  option("header", "true").\
  save("/mnt/azure-dls/staged_data/structured-streaming/staticInputDF")

# COMMAND ----------

# MAGIC %md 
# MAGIC Ahora podemos calcular la cantida de eventos "open" y "close" en na ventana de tiempo de 1 hora. para ellos vamos a agrupar la columna `action` en ventanas de tiempo de 1 hora en base a la columna `time`.

# COMMAND ----------

from pyspark.sql.functions import *      # for window() function

staticCountsDF = (
  staticInputDF
    .groupBy(
       staticInputDF.action, 
       window(staticInputDF.time, "1 hour"))    
    .count()
)
staticCountsDF.cache()


# COMMAND ----------

# MAGIC %md Una vez calculada podemos crear una nueva tabla para poder consultarla con **Spark SQL**

# COMMAND ----------

# Register the DataFrame as table 'static_counts'
staticCountsDF.createOrReplaceTempView("static_counts")

# COMMAND ----------

# MAGIC %sql select action, sum(count) as total_count from static_counts group by action

# COMMAND ----------

# MAGIC %md Ahora vamos a visualizar una serie temporal de la cantidad de eventos en una ventana de 1 hora.

# COMMAND ----------

# MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from static_counts order by time, action

# COMMAND ----------

# MAGIC %md Como paso final, podemos programar para que este Notebook se ejecute diariamente, ello implicaria que todos los días:
# MAGIC  * Se recorreria la carpeta designada
# MAGIC  * Se abrirían TODOS los json
# MAGIC  * Se haría un dataframe histórica
# MAGIC  * Se lo escribiría en una tabla
# MAGIC  * Se lo escribiría en otras zonas del Data Lake