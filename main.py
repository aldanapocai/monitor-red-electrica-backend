import asyncio, json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from influx_client import query_api, bucket  
from pydantic import BaseModel

from mqtt_bridge import mqtt_to_ws, publish_cmd 

app = FastAPI(title="Monitoreo Corriente API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# --- Cola global para fan-out ---
queue: asyncio.Queue[str] = asyncio.Queue()
clients: set[WebSocket] = set()

@app.on_event("startup")
async def startup():
    # dispara la task MQTT en background
    asyncio.create_task(mqtt_to_ws(queue))
    # dispara otra task que lee de la queue y envía a websockets
    asyncio.create_task(broadcaster())

async def broadcaster():
    while True:
        payload = await queue.get()
        if clients:  # envía solo si hay alguien conectado
            dead = set()
            for ws in clients:
                try:
                    await ws.send_text(payload)
                except WebSocketDisconnect:
                    dead.add(ws)
            clients.difference_update(dead)

@app.websocket("/live")
async def websocket_live(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()   # no esperamos nada del cliente
    except WebSocketDisconnect:
        clients.remove(ws)

@app.get("/ping")
async def ping():
    return {"pong": True}


@app.get("/api/historico")
async def historico(
    fase: str,
    start: str = Query("-24h"),   # acepta “2025-05-01T00:00:00Z” o “-1h”
    stop:  str | None = None,
):
    """Devuelve lecturas históricas de una fase."""
    flux = f'''
      from(bucket: "{bucket}")
      |> range(start: {start}, stop: {stop or "now()"})
      |> filter(fn: (r) => r._measurement == "mqtt_consumer")
      |> filter(fn: (r) => r.topic == "casa/{fase}/corriente")
      |> filter(fn: (r) => r._field == "I")
      |> keep(columns: ["_time","_value"])
      |> sort(columns: ["_time"])
    '''
    try:
        tables = query_api.query(flux)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    data = [
        {"ts": row.get_time().isoformat(), "I": row.get_value()}
        for table in tables for row in table.records
    ]
    return data

class Cmd(BaseModel):
    fase: str
    cmd: str

@app.post("/api/cmd")
async def api_cmd(payload: Cmd):
    try:
        await publish_cmd(payload.fase, payload.cmd)
        return {"status": "sent"}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))