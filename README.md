# 🛵 Delivery distribuido

Sistema distribuido que **despacha pedidos de delivery a repartidores**. Los pedidos entran a una
cola, y cada repartidor (un hilo) toma el siguiente pedido libre, va al restaurante, lo lleva al
cliente y avisa su posición en tiempo real.

## ¿Dónde se cumple cada requisito?

| Requisito | Dónde |
|---|---|
| **Manejo de hilos** | `worker.py`: cada repartidor es un `threading.Thread`. `api.py`: un hilo en segundo plano (`escuchar_eventos`) + un `threading.Lock` para que ese hilo y las peticiones no pisen los mismos datos. |
| **Tecnología de objetos** | **GraphQL** (librería Strawberry) en `api.py`: los objetos `Restaurante`, `Repartidor` y `Pedido` viajan por la red con un esquema tipado. |
| **Manejo de colas** | **RabbitMQ**: cola `pedidos` (API → repartidores) y cola `eventos` (repartidores → API). |

## Arquitectura

```
 cliente (GraphiQL o la web) ── GraphQL ──►  api.py
                                              │ crearPedido → publica en la cola "pedidos"
                                              │ hilo de fondo ← lee la cola "eventos"
                                              ▼
                          RabbitMQ (Docker): colas "pedidos" y "eventos"
                                              ▲
                          worker.py: hilo Ana · hilo Luis · hilo Sofi  (1 hilo = 1 repartidor)
```

Son **3 procesos independientes** que solo se hablan por red. Se pueden agregar más workers, incluso
en otras computadoras, y RabbitMQ reparte los pedidos entre todos.

Estados de un pedido: `EN_COLA → RECOGIENDO → EN_CAMINO → ENTREGADO`.
Estados de un repartidor: `LIBRE`, `RECOGIENDO`, `ENTREGANDO`.
La ciudad es una cuadrícula de calles de 0 a 8; los repartidores avanzan una cuadra cada 0,5 s.

## Requisitos previos

- Docker (para RabbitMQ)
- Python 3.10 o más nuevo

## Cómo correrlo

**Terminal 1: RabbitMQ y la API**

```bash
docker compose up -d
python3 -m venv .venv
source .venv/bin/activate        # en Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn api:app --port 8000
```

**Terminal 2: los repartidores**

```bash
source .venv/bin/activate        # en Windows: .venv\Scripts\activate
python worker.py Ana Luis Sofi
```

## Demo paso a paso

1. **GraphiQL**: abre <http://localhost:8000/graphql>. Es una página para probar consultas, como un
   Postman ya incluido. Prueba:

   ```graphql
   { repartidores { nombre x y estado }  pedidos { id cliente estado repartidor } }
   ```

2. **Lluvia de pedidos**: pega esta mutación, que crea 8 pedidos de golpe:

   ```graphql
   mutation {
     a: crearPedido(cliente: "Carla", restaurante: "Pizza Roma",  x: 8, y: 8) { id }
     b: crearPedido(cliente: "Diego", restaurante: "Sushi Zen",   x: 0, y: 8) { id }
     c: crearPedido(cliente: "Elena", restaurante: "Burger Town", x: 8, y: 0) { id }
     d: crearPedido(cliente: "Fabio", restaurante: "Pizza Roma",  x: 5, y: 5) { id }
     e: crearPedido(cliente: "Gina",  restaurante: "Sushi Zen",   x: 2, y: 6) { id }
     f: crearPedido(cliente: "Hugo",  restaurante: "Burger Town", x: 6, y: 1) { id }
     g: crearPedido(cliente: "Irene", restaurante: "Pizza Roma",  x: 3, y: 8) { id }
     h: crearPedido(cliente: "Juan",  restaurante: "Sushi Zen",   x: 7, y: 7) { id }
   }
   ```

3. **Panel de RabbitMQ**: <http://localhost:15672> (usuario `delivery`, clave `delivery`) → pestaña
   *Queues* → `pedidos`. Vas a ver **Ready** (pedidos esperando), **Unacked** (pedidos que alguien
   está entregando) y **Consumers** (repartidores conectados).
4. **Escalar**: en otra terminal corre `python worker.py Pedro` y la cola se vacía más rápido.
5. **Tolerancia a fallos**: mientras un worker reparte, ciérralo con `Ctrl+C`. El pedido que llevaba
   vuelve a *Ready* y otro repartidor lo toma. Además, los repartidores de ese worker desaparecen de
   la lista a los 10 s.

## Worker en otra computadora

En la PC con RabbitMQ, averigua su IP (por ejemplo `192.168.1.10`) y deja abierto el puerto 5672 en el
firewall. En la otra PC (con este proyecto y sus dependencias instaladas):

```bash
RABBIT_URL=amqp://delivery:delivery@192.168.1.10 python worker.py Pedro
```

En Windows (PowerShell): `$env:RABBIT_URL="amqp://delivery:delivery@192.168.1.10"; python worker.py Pedro`

## Cómo funciona por dentro

- **`crearPedido`** valida los datos, publica el pedido en `pedidos` y lo guarda como `EN_COLA`.
  Publicar y guardar ocurren dentro del `lock`, así el hilo de eventos nunca recibe noticias de un
  pedido que todavía no está guardado.
- **Cada hilo del worker** abre su propia conexión a RabbitMQ (la librería `pika` no se puede
  compartir entre hilos) y pide de a un pedido (`prefetch_count=1`).
- **El `ack`** (la confirmación a RabbitMQ) se manda **recién al entregar**. Si el worker muere a
  mitad de camino, RabbitMQ nunca recibió el `ack` y le da el pedido a otro repartidor.
- **Latido**: un repartidor libre avisa su posición cada 3 s. Si la API se reinicia, en 3 s vuelve a
  conocer a todos; si un repartidor deja de avisar 10 s, la API lo saca de la lista.
- **Reconexión**: si RabbitMQ se cae, la API y los workers reintentan cada 3 s solos.
- **Cola durable + mensajes persistentes**: los pedidos en espera sobreviven a un reinicio de RabbitMQ.
- **Número de pedido al azar** (`#4821`): si fuera 1, 2, 3…, al reiniciar la API los números se
  repetirían y se mezclarían con pedidos viejos que todavía se están entregando.

**Limitación**: la API guarda el estado en memoria (no hay base de datos). Si la reinicias, la lista
de pedidos se pierde, aunque los pedidos que ya estaban en la cola igual se entregan.

## GraphQL para quien sabe REST

| REST | GraphQL |
|---|---|
| `GET /pedidos` | **Query**: `{ pedidos { id estado } }` |
| `POST /pedidos` | **Mutation**: `mutation { crearPedido(...) { id } }` |
| Muchas URLs | Una sola URL: `POST /graphql` |
| El servidor decide qué campos devuelve | El cliente pide exactamente los campos que quiere |
| Modelo Pydantic | Clase con `@strawberry.type` |
| Función con `@app.get` | Método con `@strawberry.field` (dentro de `Query`) |
| Función con `@app.post` | Método con `@strawberry.mutation` (dentro de `Mutation`) |

Llamarlo desde JavaScript es un `fetch` normal:

```js
const r = await fetch("http://localhost:8000/graphql", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ query: "{ pedidos { id estado } }" }),
});
const { data } = await r.json();
```

Strawberry convierte los nombres de Python a camelCase: `crear_pedido` en `api.py` se llama
`crearPedido` en GraphQL.
