import os, asyncio, json, ssl
from dotenv import load_dotenv
from asyncio_mqtt import Client, MqttError

load_dotenv()                                

HIVEMQ_HOST = os.getenv("MQTT_HOST")
HIVEMQ_PORT = int(os.getenv("MQTT_PORT", 8883))
HIVEMQ_USER = os.getenv("MQTT_USER")
HIVEMQ_PASS = os.getenv("MQTT_PASS")

CMD_USER = os.getenv("CMD_USER", HIVEMQ_USER)    # fallback al mismo usuario
CMD_PASS = os.getenv("CMD_PASS", HIVEMQ_PASS)

TLS_CTX = ssl.create_default_context()

async def mqtt_to_ws(queue):
    """Suscribe a casa/+/corriente y coloca los payloads en una queue."""
    while True:
        try:
            async with Client(
                hostname=HIVEMQ_HOST,
                port=HIVEMQ_PORT,
                username=HIVEMQ_USER,
                password=HIVEMQ_PASS,
                tls_context=TLS_CTX,
                client_id="backend-fastapi",
                keepalive=30,
            ) as client:
                async with client.unfiltered_messages() as messages:
                    await client.subscribe("casa/+/corriente", qos=1)
                    async for msg in messages:
                        queue.put_nowait(msg.payload.decode())
        except MqttError as e:
            print("MQTT error:", e, " – reintentando en 5 s")
            await asyncio.sleep(5)

async def publish_cmd(fase: str, cmd: str):
    """Publica un comando en casa/<fase>/cmd con QoS 1."""
    topic = f"casa/{fase}/cmd"
    async with Client(
        hostname=HIVEMQ_HOST,
        port=HIVEMQ_PORT,
        username=CMD_USER,
        password=CMD_PASS,
        tls_context=TLS_CTX,
        client_id="backend-cmd",
        keepalive=30,
    ) as client:
        await client.publish(topic, json.dumps({"cmd": cmd}), qos=1)
