from mqtt_conf import MQTTLogic
from write_influx import InfluxLogic
from queue import Queue
from threading import Lock
from os import getenv
from dotenv import load_dotenv

load_dotenv()

mqtt_broker = getenv("MQTT_BROKER")
mqtt_port = int(getenv("MQTT_PORT"))
mqtt_usr = getenv("MQTT_USER")
mqtt_pass  = getenv("MQTT_PASS")
mqtt_topics = [
    ("shellies/shellyem3-485519DC84EC/emeter/0/total", 0),
    ("shellies/shellyem3-485519DC84EC/emeter/1/total", 0),
    ("shellies/shellyem3-485519DC84EC/emeter/2/total", 0),
    ("shellies/shellyem3-C45BBE5FD50D/emeter/0/total", 0),
    ("shellies/shellyem3-C45BBE5FD50D/emeter/1/total", 0),
    ("shellies/shellyem3-C45BBE5FD50D/emeter/2/total", 0),
]

influx_token = getenv("INFLUX_TOKEN")
influx_url = getenv("INFLUX_URL")
influx_org = getenv("INFLUX_ORG")
influx_bucket = getenv("INFLUX_BUCKET")

sum_queue = Queue()
sum_lock = Lock()

def on_message(client, userdata, msg):
    topic = msg.topic
    value = float(msg.payload)
    print(f"Received {topic}: {value}")

    with sum_lock:
        sum_queue.put(value)

mqtt_module = MQTTLogic(mqtt_broker, mqtt_port, mqtt_topics, on_message, mqtt_usr, mqtt_pass)
influx_module = InfluxLogic(influx_url, influx_token, influx_org, influx_bucket)

mqtt_module.connect()
mqtt_module.start_loop()

try:
    while True:
        while sum_queue.qsize() < len(mqtt_topics):
            pass
        with sum_lock:
            total_sum = sum(sum_queue.queue)

        influx_module.write_data(total_sum)
        sum_queue.queue.clear()

except KeyboardInterrupt:
    pass
finally:
    mqtt_module.disconnect()
    influx_module.close()
