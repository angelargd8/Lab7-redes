import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from matplotlib.animation import FuncAnimation
import sys
import json

TOPIC = "22243" 
BOOTSTRAP = "147.182.219.133:9092"

# Mapa inverso para decodificar dirección del viento
VIENTO_REVERSE = {
    0: "N", 1: "NO", 2: "O", 3: "SO",
    4: "S", 5: "SE", 6: "E", 7: "NE"
}

def decode_payload_compressed(payload_bytes):
    """
    Decodifica 3 bytes (24 bits) a los datos originales.
    """
    if len(payload_bytes) != 3:
        raise ValueError(f"Payload comprimido debe ser 3 bytes, recibido: {len(payload_bytes)}")
    
    packed = (payload_bytes[0] << 16) | (payload_bytes[1] << 8) | payload_bytes[2]
    
    viento_idx = packed & 0b111
    humedad = (packed >> 3) & 0b1111111
    temp_int = (packed >> 10) & 0b11111111111111
    
    temp = temp_int / 100.0
    viento = VIENTO_REVERSE.get(viento_idx, "?")
    
    return {
        "temp": round(temp, 2),
        "humedad": humedad,
        "viento": viento,
        "formato": "COMPRIMIDO (3 bytes)"
    }

def decode_payload_json(payload_bytes):
    """
    Decodifica JSON tradicional.
    """
    try:
        data = json.loads(payload_bytes.decode('utf-8'))
        data["formato"] = "JSON"
        return data
    except:
        raise ValueError("No se pudo decodificar como JSON")

def decode_payload_auto(payload_bytes):
    """
    Detecta automáticamente el formato y decodifica.
    """
    # Intentar JSON primero (comienza con '{')
    if len(payload_bytes) > 0 and payload_bytes[0] == 123:  # '{' = 123 en ASCII
        try:
            return decode_payload_json(payload_bytes)
        except:
            pass
    
    # Intentar formato comprimido (3 bytes)
    if len(payload_bytes) == 3:
        try:
            return decode_payload_compressed(payload_bytes)
        except:
            pass
    
    raise ValueError(f"Formato desconocido: {len(payload_bytes)} bytes")


# Configurar consumidor Kafka
print("Configurando consumidor Kafka híbrido...")
try:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="grupo_labo7_hybrid",
        value_deserializer=lambda v: v,  # Recibir bytes crudos
        consumer_timeout_ms=1000
    )
    print("✓ Consumidor conectado correctamente\n")
except Exception as e:
    print(f"✗ Error conectando al broker Kafka: {e}")
    sys.exit(1)


# Listas para almacenar histórico
all_temp = []
all_hume = []
all_wind = []
all_formats = []
contador_mensajes = 0


def actualizar(frame):
    """Función para actualizar la gráfica."""
    global contador_mensajes
    
    try:
        mensaje = next(consumer)
        payload_bytes = mensaje.value
        
        print(f"\n{'='*70}")
        print(f"Mensaje #{contador_mensajes + 1} recibido:")
        print(f"  Tamaño: {len(payload_bytes)} bytes")
        
        if len(payload_bytes) <= 10:
            print(f"  Raw (hex): {payload_bytes.hex().upper()}")
            print(f"  Raw (dec): {list(payload_bytes)}")
        else:
            print(f"  Raw (inicio): {payload_bytes[:20].hex().upper()}...")
        
        # Decodificar automáticamente
        data = decode_payload_auto(payload_bytes)
        
        print(f"  Datos decodificados:")
        print(f"     Temperatura: {data['temp']}°C")
        print(f"     Humedad: {data['humedad']}%")
        print(f"     Viento: {data['viento']}")
        print(f"{'='*70}")
        
        # Almacenar datos
        all_temp.append(data["temp"])
        all_hume.append(data["humedad"])
        all_wind.append(data["viento"])
        all_formats.append(data["formato"])
        contador_mensajes += 1
        
        # Actualizar gráfica
        plt.cla()
        
        plt.subplot(2, 1, 1)
        plt.plot(all_temp,  marker='o', 
                 linewidth=2, markersize=4)
        plt.ylabel("Temperatura (°C)", fontsize=10)
        plt.legend(loc="upper left")
        plt.grid(True, alpha=0.3)
        plt.title(f"Telemetría IoT (Híbrido) | Mensajes: {contador_mensajes} | Formato: {data['formato']}", 
                  fontsize=11, fontweight='bold')
        
        plt.subplot(2, 1, 2)
        plt.plot(all_hume, label="Humedad (%)", color="blue", marker='s', 
                 linewidth=2, markersize=4)
        plt.xlabel("Muestra #", fontsize=10)
        plt.ylabel("Humedad (%)", fontsize=10)
        plt.legend(loc="upper left")
        plt.grid(True, alpha=0.3)
        
        plt.figtext(0.99, 0.01, f"Último viento: {data['viento']}", 
                    ha='right', fontsize=10, 
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
    except StopIteration:
        pass
    except ValueError as e:
        print(f"  ✗ Error de decodificación: {e}")
    except Exception as e:
        print(f"  ✗ Error inesperado: {e}")


# Iniciar aplicación
print("\n" + "="*70)
print("CONSUMER HÍBRIDO DE TELEMETRÍA")
print("="*70)
print(f"Topic: {TOPIC}")
print(f"Broker: {BOOTSTRAP}")
print("Formatos soportados:")
print("  • JSON tradicional (45+ bytes)")
print("  • Comprimido (3 bytes): [14 bits temp][7 bits hum][3 bits viento]")
print("="*70)
print("\nEsperando mensajes...\n")

try:
    fig = plt.figure(figsize=(12, 8))
    ani = FuncAnimation(fig, actualizar, interval=2000, cache_frame_data=False)
    plt.show()
    
except KeyboardInterrupt:
    print("\n\n✓ Consumer detenido por el usuario")
    consumer.close()
    
except Exception as e:
    print(f"\n✗ Error fatal: {e}")
    consumer.close()
    sys.exit(1)