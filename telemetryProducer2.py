import random
import json
import time
import numpy as np
from kafka import KafkaProducer

# temperatura
TEMP_MEAN = 55  
TEMP_STD  = 15  

# humedad
HUM_MEAN = 50
HUM_STD  = 20

# Mapa de direcciones del viento a índices (3 bits = 0-7)
VIENTO_MAP = {
    "N": 0, "NO": 1, "O": 2, "SO": 3,
    "S": 4, "SE": 5, "E": 6, "NE": 7
}

def generar_temperatura():
    valor = np.random.normal(TEMP_MEAN, TEMP_STD)
    valor = max(0, min(110, valor))
    return round(valor, 2)

def generar_humedad():
    valor = np.random.normal(HUM_MEAN, HUM_STD)
    valor = max(0, min(100, valor))
    return int(round(valor))

def generar_direccion_viento():
    dir_viento = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]
    return random.choice(dir_viento)

def encode_payload(temp, humedad, viento):
    """
    Codifica los datos en 3 bytes (24 bits):
    - 14 bits: temperatura (temp * 100, rango 0-11000)
    - 7 bits: humedad (rango 0-100)
    - 3 bits: dirección viento (0-7)
    
    Retorna: bytes de 3 caracteres
    """
    # Temperatura: multiplicar por 100 para mantener 2 decimales
    temp_int = int(temp * 100)
    temp_int = max(0, min(11000, temp_int))  # clamp a 14 bits max
    
    # Humedad: ya es entero 0-100
    humedad = max(0, min(100, humedad))
    
    # Viento: convertir a índice 0-7
    viento_idx = VIENTO_MAP[viento]
    
    # Empaquetar en 24 bits
    # [14 bits temp][7 bits humedad][3 bits viento]
    packed = (temp_int << 10) | (humedad << 3) | viento_idx
    
    # Convertir a 3 bytes
    byte1 = (packed >> 16) & 0xFF
    byte2 = (packed >> 8) & 0xFF
    byte3 = packed & 0xFF
    
    return bytes([byte1, byte2, byte3])

def generar_data():
    temp = generar_temperatura()
    humedad = generar_humedad()
    viento = generar_direccion_viento()
    
    # Mostrar datos originales
    data_original = {
        "temp": temp,
        "humedad": humedad,
        "viento": viento
    }
    
    # Codificar a 3 bytes
    payload_bytes = encode_payload(temp, humedad, viento)
    
    return data_original, payload_bytes

def main(): 
    bootstrap_server = "147.182.219.133:9092"
    topic = "22243"

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: v,  
        key_serializer=lambda k: k.encode('utf-8')
    )

    print("PAYLOAD COMPRIMIDO A 3 BYTES")
    print("="*70)
    print(f"Broker: {bootstrap_server}")
    print(f"Topic: {topic}")
    print("Estructura: [14 bits temp][7 bits humedad][3 bits viento] = 24 bits")
    print("="*70)
    print("\nEnviando datos al servidor Kafka...\n")

    mensaje_num = 0
    
    while True:
        mensaje_num += 1
        data_original, payload_bytes = generar_data()
        
        # Mostrar información detallada
        print(f" MENSAJE #{mensaje_num}")
        print(f"{'='*70}")
        print(f"Datos originales:")
        print(f"   Temperatura: {data_original['temp']}°C")
        print(f"   Humedad: {data_original['humedad']}%")
        print(f"   Viento: {data_original['viento']}")
        print(f"\nPayload comprimido:")
        print(f"  Hex: {payload_bytes.hex().upper()}")
        print(f"  Dec: {list(payload_bytes)}")
        print(f"  Bytes: {repr(payload_bytes)}")
        print(f"  Tamaño: {len(payload_bytes)} bytes")
        
        # Enviar al broker
        try:
            producer.send(
                topic=topic,
                key="sensor1",
                value=payload_bytes  # ← Enviando bytes, NO JSON
            )
            producer.flush()
            print(f"\n Enviado correctamente")
        except Exception as e:
            print(f"\n Error al enviar: {e}")
        
        # Espera aleatoria entre 15 y 30 segundos
        wait_time = random.randint(15, 30)
        time.sleep(wait_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✓ Producer detenido por el usuario")
    except Exception as e:
        print(f"\n✗ Error fatal: {e}")
        import traceback
        traceback.print_exc()