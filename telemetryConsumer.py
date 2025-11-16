# consumidor
"""
Ya con la data publicada y se ha generado en topics
el consumer se suscribe al topic y esucucha los mensajes entrantes
siendo el consumidor el que tiene el rol de los elementos
que estan detras del edge.

"""
import matplotlib.pyplot as plt
import json
from kafka import KafkaConsumer
import json
from matplotlib.animation import FuncAnimation



TOPIC = "22243" 
BOOTSTRAP = "147.182.219.133:9092"

# consumidor kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",   # leer desde el principio
    enable_auto_commit=True,
    group_id="grupo_labo7",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


# listas para graficar
all_temp = []
all_hume = []
all_wind = []


# actualizar y graficar
def actualizar(frame):
    try:
        mensaje = next(consumer)
        payload = mensaje.value

        print("Recibido:", payload)

        # Guardar data
        all_temp.append(payload["temp"])
        all_hume.append(payload["humedad"])
        all_wind.append(payload["viento"])

        # Limpiar gráfica
        plt.cla()

        # Temperatura
        plt.plot(all_temp, label="Temperatura (°C)", color="red")

        # Humedad
        plt.plot(all_hume, label="Humedad (%)", color="blue")

        plt.legend(loc="upper left")
        plt.title("Telemetría – Datos desde Kafka Broker")
        plt.xlabel("Muestras recibidas")
        plt.ylabel("Valor")
        plt.tight_layout()

    except StopIteration:
        pass


# graficar 
fig = plt.figure()
ani = FuncAnimation(fig, actualizar, interval=2000)  # 2 segundos entre actualizaciones

print("Escuchando mensajes del topic:", TOPIC)
plt.show()