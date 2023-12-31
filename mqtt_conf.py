import paho.mqtt.client as mqtt

class MQTTLogic:
    def __init__(self, broker, port, topics, on_message_callback, username, password):
        self.client = mqtt.Client()
        self.client.on_message = on_message_callback
        self.broker = broker
        self.port = port
        self.topics = topics
        self.username = username
        self.password = password

    def connect(self):
        self.client.username_pw_set(self.username, self.password)
        self.client.connect(self.broker, self.port, 60)
        self.client.subscribe(self.topics)

    def start_loop(self):
        self.client.loop_start()

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()
