#Cada 5 s lee de HDFS las métricas que calcula metricas.py y las guarda en sitio/datos.json, que es el archivo que lee la pagina

import json
import os
import subprocess
import time
from datetime import datetime

CADA = 5   # segundos entre una lectura y la siguiente
METRICAS = "/user/gonzalo/stm/metricas"
DESTINO ="/home/gonzalo/stm/web/datos.json"


def leer_metrica(nombre):
    #Lee una métrica de HDFS: una carpeta con archivos JSON, una fila por línea, devuelve una lista de filas, cada fila un diccionario
    salida = subprocess.run(
        ["hdfs", "dfs", "-cat", f"{METRICAS}/{nombre}/part-*"],
        capture_output=True, text=True, check=True,
    ).stdout
    return [json.loads(linea) for linea in salida.splitlines() if linea.strip()]


def exportar():
    total = leer_metrica("cantidad_pasajeros")[0]
    por_hora = sorted(leer_metrica("boletos_hora"), key=lambda fila: fila["hora"])
    resultado = {
        # la hora en que Spark calculo las métricas (si no esta, se pone la hora actual)
        "actualizado": total.get("calculado") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_pasajeros": total["cantidad_pasajeros"],
        "por_hora": por_hora,
    }
    # Se escribe en un archivo temporal y después se renombra:
    temporal = DESTINO + ".tmp"
    with open(temporal, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False)
    os.replace(temporal, DESTINO)
    print(datetime.now().strftime("%H:%M:%S"), "· datos.json actualizado ·",
          resultado["total_pasajeros"], "pasajeros", flush=True)


while True:
    try:
        exportar()
    except Exception as error:
        # por ejemplo, si Spark está reescribiendo las métricas justo en ese momento:
        # se deja el datos.json anterior y se vuelve a intentar en el próximo minuto
        print("No se pudo leer las métricas:", error, flush=True)
    time.sleep(CADA)
