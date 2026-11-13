import csv
import json
import sys
import time
from datetime import datetime

from kafka import KafkaProducer

# El archivo se pasa al inicar el script (python productor.py ARCHIVO.csv)
file = sys.argv[1]
speed = float(sys.argv[2]) # configurar velocidad (1 tiempo real)

# Conectarse a Kafka
kafka = KafkaProducer(bootstrap_servers="localhost:9092")

real_start = None     # hora en que se envió el primer boleto
data_start = None    # fecha_evento del primer boleto
sent = 0

# Leer el CSV
with open(file, encoding="utf-8") as f:
    for row in csv.DictReader(f): # csv.DictReader Convierte cada fila en un diccionario
        row_time = datetime.strptime(row["fecha_evento"], "%Y-%m-%d %H:%M:%S") # pasa de string a datetime

        if real_start is None:
            real_start = time.time()
            data_start = row_time

        # Esperar hasta a la hora del boleto
        seconds_data = (row_time - data_start).total_seconds()
        send_time = real_start + seconds_data / speed
        wait = send_time - time.time()
        if wait > 0:
            time.sleep(wait)

        # Enviarlo a Kafka (clave = la linea, valor = la fila como JSON)
        # Envia en JSON porque spark despues lo interpreta asi (Kafka almacena bytes)
        key = row["dsc_linea"].encode("utf-8")
        value = json.dumps(row).encode("utf-8")
        kafka.send("boletos", key=key, value=value)

        sent += 1

# Esperar a que se envien los ultimos mensajes
kafka.flush()
print("Finalizado:", sent, "boletos")
