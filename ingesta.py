from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import valkey

# Inicia sesion de spark
spark = SparkSession.builder.appName("STM Streaming").getOrCreate()

# Leer eventos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "boletos") \
    .option("startingOffsets", "earliest") \
    .load()

# Convierte bytes a string
data = df.selectExpr("CAST(value AS STRING)")

# Define el esquema para el from_json
schema = StructType([StructField("fecha_evento", TimestampType()), StructField("dsc_linea", StringType()), StructField("cantidad_pasajeros", StringType())])

# Pasa el string a JSON
tickets = data.select(from_json(col("value"), schema).alias("ticket")).select("ticket.*")

# Para que se pueda usar en las querys SQL
tickets.createOrReplaceTempView("tickets")

querys = spark.sql("""SELECT window(fecha_evento, '1 hour') AS interval, dsc_linea AS line, SUM(CAST(cantidad_pasajeros AS INTEGER)) AS passengers FROM tickets
        GROUP BY window(fecha_evento, '1 hour'), dsc_linea""")

client = valkey.Valkey(host="localhost", port=6379, decode_responses=True)

def push_valkey(batch_df, batch_id):
    print("Recibido", batch_id)
    
    rows = batch_df.collect()

    for row in rows:
        # Por cada linea separa los datos
        interval = row["interval"].start
        line = row["line"]
        passengers = row["passengers"]

        key = f"stm:{interval}:{line}"

        # Envia a valkey el intervalo y linea como key, pasajeros como value
        client.set(key, passengers)

push = querys.writeStream.outputMode("update").foreachBatch(push_valkey).start()

push.awaitTermination()