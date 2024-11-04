import os
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3.write_client.client.write_api import ASYNCHRONOUS
import json

load_dotenv()

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
print("MQTT connect...")

influx_client = InfluxDBClient3(
    host=os.getenv("INFLUXDB_HOST"),
    org=os.getenv("INFLUXDB_ORG"),
    database=os.getenv("INFLUXDB_BUCKET"),
    token=os.getenv("INFLUXDB_TOKEN"),
)

write_api = influx_client.write


with open("ressources/config.json") as config_file:
    print("Read config file...")
    config = json.load(config_file)
    topics = config["topics"]
    print(f"Config file for {config['device']} found.")
    print(topics)


def msg_power(payload):
    """Converts the payload received from the MQTT broker to power measurement"""
    measurement = float(payload)
    return measurement


def msg_energy(payload):
    """Converts the payload received from the MQTT broker to energy measurement"""
    measurement = float(payload) / 60  # watt per minute to watt per hour
    return measurement


def msg_switch_state(payload):
    """Converts the payload received from the MQTT broker to switch switch state measurement"""
    measurement = None
    if payload == "on":
        measurement = 1
    if payload == "off":
        measurement = 0
    return measurement


def msg_temperature(payload):
    """Converts the payload received from the MQTT broker to temperature measurement"""
    measurement = float(payload)
    return measurement


def msg_humidity(payload):
    """Converts the payload received from the MQTT broker to humidity measurement"""
    measurement = float(payload)
    return measurement


def msg_battery(payload):
    """Converts the payload received from the MQTT broker to battery measurement"""
    measurement = float(payload)
    return measurement


def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    print("Connected with result code " + str(rc))

    # Subscribe to the topics
    for topic in topics:
        client.subscribe(topic["topic"])


def on_message_shelly(client, userdata, msg):
    """Callback when publish message is received from MQTT Broker"""
    # print(msg.topic + " " + str(msg.payload))
    point = None
    payload = None

    for topic in topics:
        if topic["topic"] == msg.topic:
            if topic["field"] == "power":
                payload = msg_power(msg.payload)
            if topic["field"] == "energy_Wh":
                payload = msg_energy(msg.payload)
            if topic["field"] == "switch_state":
                payload = msg_switch_state(msg.payload)
            if topic["field"] == "temperature":
                payload = msg_temperature(msg.payload)
            if topic["field"] == "humidity":
                payload = msg_humidity(msg.payload)
            if topic["field"] == "battery":
                payload = msg_battery(msg.payload)
            if payload:
                point = Point(topic["measurement"]).field(topic["field"], payload)
    if point:
        print(f"Point: {point}")
        write_api(point)


def on_message_aqara(client, userdata, msg):
    """Callback when publish message is received from MQTT Broker"""
    # print(msg.topic + " " + str(msg.payload))
    point = []
    print(msg.topic)

    json_payload = json.loads(msg.payload)

    print(json_payload)

    for topic in topics:

        point.append(
            Point(topic["measurement"]).field(
                "temperature", float(json_payload["temperature"])
            )
        )
        point.append(
            Point(topic["measurement"]).field(
                "humidity", float(json_payload["humidity"])
            )
        )
        point.append(
            Point(topic["measurement"]).field(
                "pressure", float(json_payload["pressure"])
            )
        )
        point.append(
            Point(topic["measurement"]).field("battery", float(json_payload["battery"]))
        )

    for p in point:
        write_api(p)


if os.getenv("MQTT_USER") and os.getenv("MQTT_PW"):
    print("mqtt user login ...")
    mqtt_client.username_pw_set(os.getenv("MQTT_USER"), os.getenv("MQTT_PW"))

mqtt_client.on_connect = on_connect
if config["device"] == "shelly":
    mqtt_client.on_message = on_message_shelly
if config["device"] == "aqara":
    mqtt_client.on_message = on_message_aqara

mqtt_client.connect(os.getenv("MQTT_BROKER_URL"))
mqtt_client.loop_forever()
