import asyncio
import io
import json
import os
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from minio import Minio
from minio.error import S3Error

#configuration de kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders_topic")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "datalake_worker_group")

#configuration de MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("MINIO_BUCKET", "orders-datalake")

#initialisation du stockage
def init_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False, #indique qu'on communique en http en local et non en https
    )

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"[DataLake Worker] Bucket '{BUCKET_NAME}' créé dans MinIO.")
    else:
        print(f"[DataLake Worker] Bucket '{BUCKET_NAME}' déjà existant.")

    return client

#archivage et partitionnement => save_event_to_datalake
def save_event_to_datalake(minio_client: Minio, event_data: dict) -> str:
    now = datetime.now(timezone.utc)
    order_id = event_data.get("order_id", "unknown")
    timestamp_str = now.strftime("%H%M%S_%f")

    # Construction du chemin partitionné type Hive/Spark
    object_name = (
        f"raw/orders/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"order_{order_id}_{timestamp_str}.json"
    )

    # Conversion du dictionnaire en flux d'octets en mémoire (sans créer de fichier sur le disque local)
    raw_bytes = json.dumps(event_data, indent=2).encode("utf-8")
    data_stream = io.BytesIO(raw_bytes)

    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=data_stream,
        length=len(raw_bytes),
        content_type="application/json",
    )
    return object_name


async def consume_orders():
    """Boucle principale d'écoute asynchrone sur Apache Kafka."""
    minio_client = init_minio_client()

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",  # Lit les messages depuis le début si c'est la première connexion
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    await consumer.start()
    print(f"[DataLake Worker] En écoute sur le topic Kafka '{KAFKA_TOPIC}'...")

    try:
        async for msg in consumer:
            event = msg.value
            print(f"[DataLake Worker] Événement reçu : {event}")

            try:
                # Exécution de l'écriture S3 dans un thread séparé pour ne pas bloquer la boucle asyncio
                object_path = await asyncio.to_thread(
                    save_event_to_datalake, minio_client, event
                )
                print(f"[DataLake Worker] Sauvegardé dans MinIO -> {BUCKET_NAME}/{object_path}")
            except S3Error as err:
                print(f"[DataLake Worker] Erreur MinIO lors de l'archivage : {err}")

    finally:
        await consumer.stop()
        print("[DataLake Worker] Consommateur Kafka arrêté proprement.")


if __name__ == "__main__":
    try:
        asyncio.run(consume_orders())
    except KeyboardInterrupt:
        print("[DataLake Worker] Arrêt demandé par l'utilisateur.")

