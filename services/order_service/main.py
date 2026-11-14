import json
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from fastapi import FastAPI, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from aiokafka import AIOKafkaProducer

from database import engine, Base, get_db_session
from models import Order
from schemas import OrderCreate, OrderResponse

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Variable globale pour stocker l'instance du producteur Kafka
kafka_producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Gestionnaire de cycle de vie (Startup & Shutdown).
    Ce bloc remplace les anciens événements on_event('startup') dépréciés.
    """
    global kafka_producer

    # 1. Action au démarrage : Création des tables SQL si elles n'existent pas
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 2. Action au démarrage : Initialisation du producteur Kafka asynchrone
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await kafka_producer.start()
    print("[Order Service] Démarré avec succès : Base de données et Kafka connectés.")

    # L'application tourne et traite les requêtes HTTP ici
    yield

    # 3. Action à l'extinction : Fermeture propre des connexions
    if kafka_producer:
        await kafka_producer.stop()
    await engine.dispose()
    print("[Order Service] Connexions Kafka et PostgreSQL fermées proprement.")


app = FastAPI(
    title="Order Service",
    version="1.0.0",
    description="Microservice de gestion des commandes et publication d'événements Kafka",
    lifespan=lifespan
)


@app.post("/api/orders", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
        payload: OrderCreate,
        db: AsyncSession = Depends(get_db_session)
):
    """
    Crée une commande dans PostgreSQL puis publie un événement OrderCreated sur Kafka.
    """
    # 1. Écriture atomique dans la base de données PostgreSQL
    new_order = Order(
        customer_id=payload.customer_id,
        item=payload.item,
        amount=payload.amount,
        status="PENDING"
    )
    db.add(new_order)
    await db.commit()
    await db.refresh(new_order)

    # 2. Publication de l'événement dans le topic Kafka (Architecture Event-Driven)
    event_payload = {
        "event_type": "OrderCreated",
        "order_id": new_order.id,
        "customer_id": new_order.customer_id,
        "item": new_order.item,
        "amount": new_order.amount,
        "status": new_order.status
    }

    if kafka_producer:
        await kafka_producer.send_and_wait("orders_topic", event_payload)

    return new_order


@app.get("/api/orders/{order_id}", response_model=OrderResponse)
async def get_order(
        order_id: int,
        db: AsyncSession = Depends(get_db_session)
):
    """
    Récupère une commande par son identifiant unique.
    """
    query = select(Order).where(Order.id == order_id)
    result = await db.execute(query)
    order = result.scalar_one_or_none()

    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"La commande avec l'identifiant {order_id} n'existe pas."
        )
    return order