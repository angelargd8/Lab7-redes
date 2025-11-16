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

# configuracion de la distribucion normal para temperatura

TEMP_MEAN = 55  # media a la mitad del rango
TEMP_STD  = 15  # varianza moderada

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

if __name__ == "__main__":
    while True:
        lectura = generar_data()
        print(json.dumps(lectura, ensure_ascii=False))
        time.sleep(2)  # espera 2 segundos antes de la siguiente lectura