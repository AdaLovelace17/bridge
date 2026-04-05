import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime
import json

# ---------- MongoDB ----------
MONGO_URI = "mongodb+srv://HenHouse_db_user:Mouna1817@cluster0.cgzyo5y.mongodb.net/"
mongo_client = MongoClient(MONGO_URI)

db = mongo_client["HenHouse_db"]
collection = db["sensors_log"]

# ---------- MQTT ----------
MQTT_BROKER = "w150161e.ala.eu-central-1.emqxsl.com"
MQTT_PORT = 8883
MQTT_USER = "HenHouse"
MQTT_PASS = "Mouna1817"

TOPIC_SENSORS = "mouna/farm/sensors"
TOPIC_COMMANDS = "mouna/farm/app_commands"

def on_connect(client, userdata, flags, rc):

    print("Connected with code:", rc)

    client.subscribe(TOPIC_SENSORS)
    client.subscribe(TOPIC_COMMANDS)

def on_message(client, userdata, msg):

    try:

        topic = msg.topic
        payload = msg.payload.decode()

        print("Message received:", topic, payload)

        if topic == TOPIC_SENSORS:

            data = json.loads(payload)

            data["created_at"] = datetime.utcnow()

            collection.insert_one(data)

            print("Saved to MongoDB")

        elif topic == TOPIC_COMMANDS:

            print("Command from app:", payload)

            client.publish("mouna/farm/arduino_actuators", payload)

    except Exception as e:

        print("Error:", e)

client = mqtt.Client()

client.username_pw_set(MQTT_USER, MQTT_PASS)

client.on_connect = on_connect
client.on_message = on_message

client.tls_set()

client.connect(MQTT_BROKER, MQTT_PORT)

client.loop_forever()