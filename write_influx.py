import influxdb_client,  time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from os import getenv
from dotenv import load_dotenv

load_dotenv()

token = getenv("INFLUXDB_TOKEN")
org = getenv("INFLUX_ORG")
url = getenv("INFLUX_URL")
bucket = getenv("INFLUX_BUCKET")

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

write_api = write_client.write_api(write_options=SYNCHRONOUS)
   
def send_data(value):
    point = (
    Point("energia_total")
    .tag("energia", "energia")
    .field("energia", value)
    )
    write_api.write(bucket=bucket, org=org, record=point)
