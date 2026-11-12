from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# La sesion de Spark
spark = (SparkSession.builder
         .appName("boletos-en-vivo")
         .master("yarn")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

# Leer el topic de kafka
boletos_kafka = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", "localhost:9092")
                 .option("subscribe", "boletos")
                 .load())

# Pasar de bytes a las columnas de CSV
columnas = ["id_viaje", "con_tarjeta", "fecha_evento", "tipo_viaje", "descripcion_tipo_viaje",
            "grupo_usuario", "descripcion_grupo_usuario", "grupo_usuario_especifico",
            "descripcion_grupo_usuario_espe", "ordinal_de_tramo", "cantidad_pasajeros",
            "codigo_parada_origen", "cod_empresa", "descrip_empresa", "linea_codigo",
            "dsc_linea", "sevar_codigo"]
esquema = StructType([StructField(c, StringType()) for c in columnas]) # Crea cada campo de una fila tomando el array columnas

boletos = (boletos_kafka
           .select(from_json(col("value").cast("string"), esquema).alias("b"),
                   col("timestamp").alias("enviado"))
           .select("b.*", "enviado"))

# Guarda en HDFS como Parquet
archivo = (boletos.writeStream
           .format("parquet")
           .option("path", "/user/gonzalo/stm/boletos_vivo")
           .option("checkpointLocation", "/user/gonzalo/checkpoints/archivo")
           .trigger(processingTime="5 seconds")
           .start())

# Queda corriendo
archivo.awaitTermination()
