from paho.mqtt import client as mqtt_client
from dotenv import load_dotenv
from os import getenv
from datetime import datetime
from write_influx import send_data

load_dotenv()

USERNAME = getenv("MQTT_USER")
PASSWORD = getenv("MQTT_PASS")
BROKER = getenv("MQTT_BROKER")
PORT = int(getenv("MQTT_PORT"))
BASE_TOPIC = getenv("MQTT_BASE_TOPIC")
SHELLY_1 = getenv("SHELLY_1")
SHELLY_2 = getenv("SHELLY_2")

devices = {"shellyem3-485519DC84EC": 0, "shellyem3-C45BBE5FD50D": 0}
count = 0
total = 0.0

def connect_mqtt() -> mqtt_client:
    def on_connect(client, usedata, flags, rc):
        if rc == 0:
            print('Conectado a Broker MQTT')
        else:
            print('La conexion al Broker ha fallado')

    client = mqtt_client.Client("consumoAC_id")
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.connect(BROKER, PORT)
    return client

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        global count
        global devices
        global total
        device = msg.topic.split("/")[1]
        if count < 6:
            devices[device] += float(msg.payload.decode())
            total += float(msg.payload.decode())
        else:
            # Llama a send_data(total) antes de reiniciar total y count
            send_data(total)
            count = 0
            total = 0
            devices = {"shellyem3-485519DC84EC": 0, "shellyem3-C45BBE5FD50D": 0}

        count += 1
        print(total)

    client.subscribe([
         (BASE_TOPIC + SHELLY_2 + "emeter/+/total", 0),
         (BASE_TOPIC + SHELLY_1 + "emeter/+/total", 0),
     ])
    client.on_message = on_message

def run_client():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

run_client()
