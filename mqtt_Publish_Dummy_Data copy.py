import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime, date, time, timedelta
import time
import geopandas
import requests
# from geoalchemy2.types import Geometry
from shapely.geometry import Point
import urllib
# import pandas as pd
# from sqlalchemy import true

#====================================================
# MQTT Settings 
MQTT_Broker = "0.0.0.0"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic_IOT_Data = "IOT/Vehicle/Data"
#====================================================

def on_connect(client, userdata, rc):
	if rc != 0:
		pass
		print ("Unable to connect to MQTT Broker...")
	else:
		print ("Connected with MQTT Broker: " , str(MQTT_Broker))

def on_publish(client, userdata, mid):
	pass
		
def on_disconnect(client, userdata, rc):
	if rc !=0:
		pass
		
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))		

		
def publish_To_Topic(topic, message):
	mqttc.publish(topic,message)
	print ("Published: " , str(message) , " " ,  "on MQTT Topic: " , str(topic))
	print ("")


#====================================================
# FAKE SENSOR 
# Dummy code used as Fake Sensor to publish some random values
# to MQTT Broker

i = 0

def publish_Fake_Sensor_Values_to_MQTT():
	# global toggle
	# if toggle == 0:
	#  - timedelta(hours=2)
	x = 31.4476154
	y = 30.2211634
	id = 203
	while True:
		id += 1
		# device_id = int(random.uniform(0, 2))
		device_id = 0
		Sensor_time = (datetime.today()).strftime("%Y,%m,%d %H:%M:%S")
		Engine_status = 1
		# int(random.uniform(0, 2))
		if Engine_status == 1:
			Working_Hours = float("{0:.2f}".format(random.uniform(1, 24)))
			Battery_Level = int(random.uniform(25, 100))
			Device_Tampering = int(random.uniform(0, 2))
			Engine_RPM = float("{0:.1f}".format(random.uniform(1, 6000)))
			x += 0.0009000
			y += 0.0009000
		else:
			Working_Hours = 0
			Battery_Level = 0
			Device_Tampering = int(random.uniform(0, 2))
			Engine_RPM = 0
			x = 31.343075
			y = 30.075943

		features=[
				{
					"attributes":{
					"deviceid": str(device_id),
					"datetime": str(Sensor_time),
					"enginestatus": str(Engine_status),
					"workinghours": str(Working_Hours),
					"batterylevel": str(Battery_Level),
					"devicetampering": str(Device_Tampering),
					"enginerpm": str(Engine_RPM),
					"longitude": str(x),
					"latitude": str(y)
					},
					"geometry" : {
					"x" : str(x),
					"y" : str(y)
					}
				}
			]
		print(f"deviceid: {device_id},\n datetime: {Sensor_time},\n enginestatus: {Engine_status},\n workinghours: {Working_Hours},\n batterylevel: {Battery_Level},\n devicetampering: {Device_Tampering},\n enginerpm: {Engine_RPM},\n longitude: {x},\n latitude: {y}")

		url= 'https://192.168.10.17/server/rest/services/iot/FeatureServer/0/addFeatures/'
		headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
		params = urllib.parse.urlencode({'f': 'json', 'features': json.dumps(features)})
		r = requests.post(url, verify=False ,data= params, headers= headers)
		print(f"{r.text}")

		sensor_data= {}
		sensor_data['OBJECTID'] = id
		sensor_data['deviceid'] = device_id
		sensor_data['DateTime'] = Sensor_time
		sensor_data['EngineStatus'] = Engine_status
		sensor_data['WorkingHours'] = Working_Hours
		sensor_data['BatteryLevel'] = Battery_Level
		sensor_data['DeviceTampering'] = Device_Tampering
		sensor_data['EngineRPM'] = Engine_RPM
		sensor_data['longitude'] = x
		sensor_data['latitude'] = y
		
		Sensor_json_data = json.dumps(sensor_data)

		print ("Publishing fake IOT Values: ", str(id), str(device_id), str(Sensor_time), str(Engine_status), str(Working_Hours), str(Battery_Level), str(Device_Tampering), str(Engine_RPM), x, y)
		publish_To_Topic (MQTT_Topic_IOT_Data, Sensor_json_data)
		
		time.sleep(60)
	# elif toggle ==1:
	# 	Working_Hours = float("{0:.2f}".format(random.uniform(0, 24)))

	# 	Working_Data = {}
	# 	Working_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
	# 	Working_Data['WorkingHours'] = Working_Hours
	# 	Working_json_data = json.dumps(Working_Data)

	# 	print ("Publishing fake Working Hours Value: " , str(Working_Hours) , "...")
	# 	publish_To_Topic (MQTT_Topic_Working_Hours, Working_json_data)
	# 	toggle = 2

	# elif toggle ==2:
	# 	Battery_Level = int(random.uniform(25, 100))

	# 	Battery_Data = {}
	# 	Battery_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
	# 	Battery_Data['BatteryLevel'] = Battery_Level
	# 	Battery_json_data = json.dumps(Battery_Data)

	# 	print ("Publishing fake BatteryLevel Value: " , str(Battery_Level) , "...")
	# 	publish_To_Topic (MQTT_Topic_Bettery_Level, Battery_json_data)
	# 	toggle = 3

	# elif toggle ==3:
	# 	Device_Tampering = int(random.uniform(0, 2))

	# 	Tampering_data = {}
	# 	Tampering_data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
	# 	Tampering_data['DeviceTampering'] = Device_Tampering
	# 	Tampering_json_data = json.dumps(Tampering_data)

	# 	print ("Publishing fake Device Tampering Value: " , str(Device_Tampering) , "...")
	# 	publish_To_Topic (MQTT_Topic_Device_Tampering, Tampering_json_data)
	# 	toggle = 4

	# elif toggle ==4:
	# 	Engine_RPM = float("{0:.1f}".format(random.uniform(0, 6)))

	# 	RPM_Data = {}
	# 	RPM_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
	# 	RPM_Data['EngineRPM'] = Engine_RPM
	# 	RPM_json_data = json.dumps(RPM_Data)

	# 	print ("Publishing fake Engine RPM Value: " , str(Engine_RPM) , "...")
	# 	publish_To_Topic (MQTT_Topic_Engine_RPM, RPM_json_data)
	# 	toggle = 5

	# elif toggle ==5:
	# 	x = float("{0:.1f}".format(random.uniform(0, 6)))
	# 	y = float("{0:.1f}".format(random.uniform(0, 6)))

	# 	location_data = {}
	# 	location_data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
	# 	location_data['longitude'] = x
	# 	location_data['latitude'] = y
	# 	location_json_data = json.dumps(RPM_Data)

	# 	print ("Publishing fake Engine RPM Value: " , str(x) , str(y) ,  "...")
	# 	publish_To_Topic (MQTT_Topic_Engine_RPM, location_json_data)
	# 	toggle = 0


		

publish_Fake_Sensor_Values_to_MQTT()

#====================================================
