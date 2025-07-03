import os
import asyncio
import json
import ssl
from dotenv import load_dotenv
from aiomqtt import Client, MqttError

load_dotenv()

# --- Credenciales y parámetros del broker HiveMQ Cloud ---
HIVEMQ_HOST = os.getenv("MQTT_HOST")
HIVEMQ_PORT = int(os.getenv("MQTT_PORT", 8883))
HIVEMQ_USER = os.getenv("MQTT_USER")
HIVEMQ_PASS = os.getenv("MQTT_PASS")

# Usuario alternativo para comandos (opcional)
CMD_USER = os.getenv("CMD_USER", HIVEMQ_USER)
CMD_PASS = os.getenv("CMD_PASS", HIVEMQ_PASS)

# --- TLS / CA ---
CA_PATH = os.getenv("MQTT_TLS_CA", "certs/hivemq-root-ca.pem")
if not os.path.isfile(CA_PATH):
    raise FileNotFoundError(f"❌ CA file no encontrado en: {CA_PATH}")

TLS_CTX = ssl.create_default_context(cafile=CA_PATH)

# ------------------------------------------------------------------
# Suscriptor principal: pasa mensajes MQTT -> cola asyncio -> websockets
# ------------------------------------------------------------------
async def mqtt_to_ws(queue: asyncio.Queue[str]):
    """Se suscribe a casa/+/corriente y coloca los payloads enriquecidos en la queue."""
    while True:
        try:
            async with Client(
                hostname=HIVEMQ_HOST,
                port=HIVEMQ_PORT,
                username=HIVEMQ_USER,
                password=HIVEMQ_PASS,
                tls_context=TLS_CTX,
                keepalive=30,
            ) as client:
                print("[mqtt_to_ws] Conectado a HiveMQ Cloud ✅")

                await client.subscribe("casa/+/corriente", qos=1)
                print("[mqtt_to_ws] Suscripto a 'casa/+/corriente' ✅")

                async for msg in client.messages:
                    payload_str = msg.payload.decode()
                    print(f"[mqtt_to_ws] Recibido MQTT → topic={msg.topic}, payload={payload_str}")

                    try:
                        payload_json = json.loads(payload_str)
                        enriched = {
                            **payload_json,
                            "topic": str(msg.topic)
                        }
                        queue.put_nowait(json.dumps(enriched))
                    except json.JSONDecodeError:
                        print("[mqtt_to_ws] ❌ Payload inválido, no es JSON:", payload_str)

        except MqttError as e:
            print("MQTT error:", repr(e), "– reintentando en 5 s")
            await asyncio.sleep(5)

# ------------------------------------------------------------------
# Publicador de comandos: HTTP POST -> MQTT
# ------------------------------------------------------------------
async def publish_cmd(fase: str, cmd: str):
    """Publica un comando en casa/<fase>/cmd con QoS 1."""
    topic = f"casa/{fase}/cmd"
    async with Client(
        hostname=HIVEMQ_HOST,
        port=HIVEMQ_PORT,
        username=CMD_USER,
        password=CMD_PASS,
        tls_context=TLS_CTX,
        keepalive=30,
    ) as client:
        await client.publish(topic, json.dumps({"cmd": cmd}), qos=1)
        print(f"[publish_cmd] Comando publicado en {topic}: {{'cmd': '{cmd}'}}")
