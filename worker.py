"""Worker del sistema de delivery: cada repartidor es un HILO.

Cada hilo toma pedidos de la cola "pedidos", camina por la ciudad y avisa su
posición y el estado del pedido en la cola "eventos".

Uso:  python worker.py Ana Luis Sofi
"""
import json
import os
import random
import sys
import threading
import time

import pika

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://delivery:delivery@localhost")


def avisar(canal, evento: dict) -> None:
    canal.basic_publish(exchange="", routing_key="eventos", body=json.dumps(evento))


def caminar(conexion, canal, nombre: str, desde: tuple, hasta: tuple, estado: str) -> tuple:
    """Avanza una cuadra cada 0,5 s (primero en x, luego en y) y devuelve la posición final."""
    x, y = desde
    while (x, y) != hasta:
        if x != hasta[0]:
            x += 1 if hasta[0] > x else -1
        else:
            y += 1 if hasta[1] > y else -1
        conexion.sleep(0.5)  # como time.sleep, pero mantiene viva la conexión con RabbitMQ
        avisar(canal, {"tipo": "repartidor", "nombre": nombre, "x": x, "y": y, "estado": estado})
    return x, y


def repartidor(nombre: str) -> None:
    """Lo que hace cada hilo: repartir para siempre y reconectarse si RabbitMQ se cae."""
    while True:
        try:
            repartir(nombre)
        except pika.exceptions.AMQPError:
            print(f"[{nombre}] sin conexión con RabbitMQ, reintento en 3 s...")
            time.sleep(3)


def repartir(nombre: str) -> None:
    conexion = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))  # una conexión por hilo
    canal = conexion.channel()
    canal.queue_declare(queue="pedidos", durable=True)
    canal.queue_declare(queue="eventos", durable=True)
    canal.basic_qos(prefetch_count=1)  # cada repartidor lleva un solo pedido a la vez
    pos = (random.randint(0, 8), random.randint(0, 8))
    print(f"[{nombre}] listo en la esquina {pos}")

    for metodo, _, cuerpo in canal.consume("pedidos", inactivity_timeout=3):
        if metodo is None:  # 3 s sin pedidos: aviso que sigo libre (latido)
            avisar(canal, {"tipo": "repartidor", "nombre": nombre, "x": pos[0], "y": pos[1], "estado": "LIBRE"})
            continue

        p = json.loads(cuerpo)
        print(f"[{nombre}] tomó el pedido #{p['id']} ({p['restaurante']} → {p['x']},{p['y']})")
        avisar(canal, {"tipo": "pedido", "id": p["id"], "estado": "RECOGIENDO", "repartidor": nombre})
        pos = caminar(conexion, canal, nombre, pos, (p["rx"], p["ry"]), "RECOGIENDO")
        avisar(canal, {"tipo": "pedido", "id": p["id"], "estado": "EN_CAMINO", "repartidor": nombre})
        pos = caminar(conexion, canal, nombre, pos, (p["x"], p["y"]), "ENTREGANDO")
        avisar(canal, {"tipo": "pedido", "id": p["id"], "estado": "ENTREGADO", "repartidor": nombre})
        # Recién ahora RabbitMQ borra el pedido. Si el worker muere antes, el pedido vuelve a la cola.
        canal.basic_ack(metodo.delivery_tag)
        avisar(canal, {"tipo": "repartidor", "nombre": nombre, "x": pos[0], "y": pos[1], "estado": "LIBRE"})
        print(f"[{nombre}] entregó el pedido #{p['id']}")


if __name__ == "__main__":
    nombres = sys.argv[1:] or ["Ana", "Luis", "Sofi"]
    for nombre in nombres:
        threading.Thread(target=repartidor, args=(nombre,), daemon=True).start()
    try:
        while True:  # el hilo principal solo espera; Ctrl+C detiene el worker
            time.sleep(1)
    except KeyboardInterrupt:
        print("Worker detenido")
