# productor

"""
crear un productor que va a generar la siguiente data de forma random sguiendo la distribución normal: 
- sensor de temperatura (termometro)
Rango: [0, 110.00]°C. Float de dos decimales
- sensor de humedad relativa (higrómetro)
Rango: [0, 100]%. Entero.
- sensor de direccion del vienteo 
{N, NO, O, SO, S, SE, E, NE} 

"""
import random
import json
import time
import numpy as np
from kafka import KafkaProducer

# temperatura
TEMP_MEAN = 55  # media a la mitad del rango
TEMP_STD  = 15  # varianza moderada

# humedad
HUM_MEAN = 50
HUM_STD  = 20

# Genera temperatura con distribución normal y respeta el rango [0, 110]
def generar_temperatura():
    valor = np.random.normal(TEMP_MEAN, TEMP_STD)
    valor = max(0, min(110, valor))  # clamp
    return round(valor, 2)

#Genera humedad con distribución normal y respeta el rango [0, 100]
def generar_humedad():
    valor = np.random.normal(HUM_MEAN, HUM_STD)
    valor = max(0, min(100, valor))
    return int(round(valor))

# Genera dirección del viento uniformemente
def generar_direccion_viento():
    dir_viento = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]
    return random.choice(dir_viento)

#Crea el JSON final del sensor
def generar_data():
    data = {
        "temp": generar_temperatura(),
        "humedad": generar_humedad(),
        "viento": generar_direccion_viento()
    }
    return data

def main(): 

    # server
    bootstrap_server = "147.182.219.133:9092"
    topic = "22243"

    #kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # JSON -> bytes
        key_serializer=lambda k: k.encode('utf-8')
    )

    print("Enviando datos al servidor Kafka...\n")

    while True:
        data = generar_data()
        print("Enviando:", data)

        producer.send(
            topic=topic,
            key="sensor1",
            value=data
        )

        # Forzar envío inmediato
        producer.flush()

        # Espera entre 15 y 30 segundos
        time.sleep(random.randint(15, 30))

if __name__ == "__main__":
    main()