from copy import copy
import paho.mqtt.client as mqtt
from store_Sensor_Data_to_DB_Spectro import sensor_Data_Handler_Spectro
import json
import time

# MQTT Settings 
MQTT_Broker = "192.168.10.21"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic = "IOT/Vehicle/Data"

#Subscribe to all Sensors at Base Topic
def on_connect(self, mosq, obj, rc):
	self.subscribe(MQTT_Topic, 2)
	mqttc.on_connect = on_connect

#Save Data into DB Table
def on_message(mosq, obj, msg):
	# This is the Master Call for saving MQTT Data into DB
	# For details of "sensor_Data_Handler" function please refer "sensor_data_to_db.py"
	print ("MQTT Topic: " , msg.topic)
	print ("Data: " , msg.payload)
	print ("MQTT Data Received...")
	print("-------------------------------------")
	sensor_Data_Handler_Spectro(msg.topic, msg.payload)

def on_subscribe(mosq, obj, mid, granted_qos):
    pass

mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

# Connect
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Continue the network loop
mqttc.loop_forever()
