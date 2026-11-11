"""API GraphQL del sistema de delivery.

- La mutación crearPedido publica cada pedido en la cola "pedidos" de RabbitMQ.
- Un hilo en segundo plano escucha la cola "eventos" (lo que avisan los repartidores)
  y actualiza el estado que devuelven las consultas.

Correr con:  uvicorn api:app --port 8000
"""
import json
import os
import threading
import time
from typing import Optional

import pika
import strawberry
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://delivery:delivery@localhost")
RESTAURANTES = {"Pizza Roma": (1, 1), "Sushi Zen": (7, 2), "Burger Town": (4, 7)}


# ---------- Tipos GraphQL: los objetos que viajan entre la API y sus clientes ----------

@strawberry.type
class Restaurante:
    nombre: str
    x: int
    y: int


@strawberry.type
class Repartidor:
    nombre: str
    x: int
    y: int
    estado: str  # LIBRE | RECOGIENDO | ENTREGANDO


@strawberry.type
class Pedido:
    id: int
    cliente: str
    restaurante: str
    x: int
    y: int
    estado: str  # EN_COLA | RECOGIENDO | EN_CAMINO | ENTREGADO
    repartidor: Optional[str] = None


# ---------- Estado en memoria ----------
# Lo usan a la vez el hilo de eventos y las peticiones GraphQL, por eso se protege con un lock.

pedidos: dict[int, Pedido] = {}
repartidores: dict[str, Repartidor] = {}
lock = threading.Lock()


# ---------- RabbitMQ ----------

def publicar(cola: str, datos: dict) -> None:
    """Abre una conexión corta, publica un mensaje JSON en la cola y la cierra.
    Cola durable + mensaje persistente = el pedido sobrevive aunque RabbitMQ se reinicie."""
    conexion = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    canal = conexion.channel()
    canal.queue_declare(queue=cola, durable=True)
    canal.basic_publish(exchange="", routing_key=cola, body=json.dumps(datos),
                        properties=pika.BasicProperties(delivery_mode=2))
    conexion.close()


def al_recibir_evento(canal, metodo, propiedades, cuerpo):
    evento = json.loads(cuerpo)
    with lock:
        if evento["tipo"] == "repartidor":
            repartidores[evento["nombre"]] = Repartidor(
                nombre=evento["nombre"], x=evento["x"], y=evento["y"], estado=evento["estado"]
            )
        elif evento["id"] in pedidos:  # si la API se reinició puede llegar un pedido viejo: se ignora
            pedido = pedidos[evento["id"]]
            pedido.estado = evento["estado"]
            pedido.repartidor = evento["repartidor"]


def escuchar_eventos() -> None:
    """Hilo en segundo plano: consume la cola "eventos" para siempre (y reintenta si RabbitMQ cae)."""
    while True:
        try:
            conexion = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
            canal = conexion.channel()
            canal.queue_declare(queue="eventos", durable=True)
            canal.basic_consume(queue="eventos", on_message_callback=al_recibir_evento, auto_ack=True)
            print("Escuchando la cola 'eventos'")
            canal.start_consuming()
        except pika.exceptions.AMQPError:
            print("Esperando a RabbitMQ...")
            time.sleep(3)


# ---------- Consultas (leer) y mutaciones (cambiar) ----------

@strawberry.type
class Query:
    @strawberry.field
    def restaurantes(self) -> list[Restaurante]:
        return [Restaurante(nombre=n, x=x, y=y) for n, (x, y) in RESTAURANTES.items()]

    @strawberry.field
    def repartidores(self) -> list[Repartidor]:
        with lock:
            return list(repartidores.values())

    @strawberry.field
    def pedidos(self) -> list[Pedido]:
        with lock:
            return sorted(pedidos.values(), key=lambda p: p.id, reverse=True)


@strawberry.type
class Mutation:
    @strawberry.mutation
    def crear_pedido(self, cliente: str, restaurante: str, x: int, y: int) -> Pedido:
        cliente = cliente.strip()
        if not cliente:
            raise Exception("El cliente es obligatorio")
        if restaurante not in RESTAURANTES:
            raise Exception(f"Restaurante desconocido: {restaurante}")
        if not (0 <= x <= 8 and 0 <= y <= 8):
            raise Exception("El destino debe estar entre 0 y 8")

        rx, ry = RESTAURANTES[restaurante]
        # Publicar y guardar dentro del lock: así el hilo de eventos nunca recibe
        # noticias de un pedido que todavía no está guardado.
        with lock:
            pedido = Pedido(id=len(pedidos) + 1, cliente=cliente, restaurante=restaurante,
                            x=x, y=y, estado="EN_COLA")
            try:
                publicar("pedidos", {"id": pedido.id, "cliente": cliente, "restaurante": restaurante,
                                     "rx": rx, "ry": ry, "x": x, "y": y})
            except pika.exceptions.AMQPError:
                raise Exception("No se pudo encolar el pedido (¿RabbitMQ está corriendo?)")
            pedidos[pedido.id] = pedido
        return pedido


schema = strawberry.Schema(query=Query, mutation=Mutation)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(GraphQLRouter(schema), prefix="/graphql")

threading.Thread(target=escuchar_eventos, daemon=True).start()
