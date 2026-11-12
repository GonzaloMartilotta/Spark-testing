import csv
import json
import sys
import time
from datetime import datetime

from kafka import KafkaProducer

# El archivo se pasa al correr el script: python productor.py ARCHIVO.csv
if len(sys.argv) != 2:
    print("Uso: python productor.py ARCHIVO.csv")
    sys.exit(1)
ARCHIVO = sys.argv[1]
VELOCIDAD = 60 # 1 = tiempo real, 60 = un minuto de datos por cada segundo real

# 1. Conectarse a Kafka
productor = KafkaProducer(bootstrap_servers="localhost:9092")

inicio_real = None     # hora real en que se envió el primer boleto
inicio_datos = None    # fecha_evento del primer boleto
enviados = 0

# 2. Recorrer el CSV, una fila por vuelta
with open(ARCHIVO, encoding="utf-8") as f:
    for fila in csv.DictReader(f):
        evento = datetime.strptime(fila["fecha_evento"], "%Y-%m-%d %H:%M:%S")

        if inicio_real is None:
            inicio_real = time.time()
            inicio_datos = evento

        # 3. Esperar hasta que le toque a este boleto
        segundos_en_los_datos = (evento - inicio_datos).total_seconds()
        momento_de_enviar = inicio_real + segundos_en_los_datos / VELOCIDAD
        espera = momento_de_enviar - time.time()
        if espera > 0:
            time.sleep(espera)

        # 4. Enviarlo a Kafka: clave = la línea, valor = la fila entera como JSON
        clave = fila["dsc_linea"].encode("utf-8")
        valor = json.dumps(fila).encode("utf-8")
        productor.send("boletos", key=clave, value=valor)

        enviados += 1
        if enviados % 1000 == 0:
            print(enviados, "enviados, vamos por", fila["fecha_evento"])

# 5. Esperar a que salgan los últimos y terminar
productor.flush()
print("Terminado:", enviados, "boletos")
