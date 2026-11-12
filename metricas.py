#la sesión de Spark y las querys sobre los datos en HDFS
#La web (web.py) llama a estas funciones y las grafica

# 1 - Crea la sesion de Spark
from pyspark.sql import SparkSession
import time

spark = (SparkSession.builder
         .appName("querys")
         .master("yarn")
         .getOrCreate())

# 2 - Lee el historial de HDFS (/user/gonzalo/stm/boletos_vivo)
def actualizar():
    historial = spark.read.parquet("/user/gonzalo/stm/boletos_vivo")
    historial.createOrReplaceTempView("boletos")


def total_boletos():
    actualizar()
    spark.sql("""
        SELECT COUNT(*) AS total_boletos, current_timestamp() AS calculado FROM boletos
    """).coalesce(1).write.mode("overwrite").json("/user/gonzalo/stm/metricas/cantidad_boletos")


def boletos_por_hora():
    actualizar()
    spark.sql("""
        SELECT HOUR(fecha_evento) AS hora, COUNT(*) AS cantidad_boletos, current_timestamp() AS calculado FROM boletos
        GROUP BY HOUR(fecha_evento)
        ORDER BY HOUR(fecha_evento)
    """).coalesce(1).write.mode("overwrite").json("/user/gonzalo/stm/metricas/boletos_hora")
    
#Actualiza las metricas
while True:
    total_boletos()
    boletos_por_hora()
    print("metricas actualizadas")
    time.sleep(60)
