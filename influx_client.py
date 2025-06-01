import os, influxdb_client
from dotenv import load_dotenv

load_dotenv()

client = influxdb_client.InfluxDBClient(
    url      = os.getenv("INFLUX_URL"),
    token    = os.getenv("INFLUX_TOKEN"),
    org      = os.getenv("INFLUX_ORG"),
    timeout  = 10_000,
)

query_api = client.query_api()
bucket = os.getenv("INFLUX_BUCKET")
