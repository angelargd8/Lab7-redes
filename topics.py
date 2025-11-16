# listar topics en kafka y verificar nuestro topic
# este archivo realmente no nos sirve para el laboratorio
# solo era para verificar que el topic se haya creado correctamente
from confluent_kafka.admin import AdminClient

admin = AdminClient({"bootstrap.servers": "147.182.219.133:9092"})

metadata = admin.list_topics(timeout=5)

print("Topics disponibles:")
for t in metadata.topics:
    print(" -", t)
