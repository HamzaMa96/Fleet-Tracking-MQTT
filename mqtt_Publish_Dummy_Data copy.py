import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime, date, time, timedelta
import time
import geopandas
import requests
# from geoalchemy2.types import Geometry
from shapely.geometry import Point
import urllib
import psycopg2
# import pandas as pd
# from sqlalchemy import true
DB_Name =  "mqtt"
hostname = 'localhost'
username = 'postgres'
pws = 'password@1324'
port_id = 5432
curs = None
conn = None
#===============================================================
# Connection initialization
conn= psycopg2.connect(
			host = hostname,
			dbname = DB_Name,
			user = username,
			password = pws,
			port = port_id)

curs = conn.cursor()
#====================================================
# MQTT Settings 
MQTT_Broker = "127.0.0.1"
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
	x = 31.2647639
	y = 30.0385172
	location = "31.2647639,30.0385172"
	id = 0
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
			x = 31.2647639
			y = 30.0385172
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
					"longitude": x,
					"latitude": y
					},
					"geometry" : {
					"x" : x,
					"y" : y
					}
				}
			]
		print(f"deviceid: {device_id},\n datetime: {Sensor_time},\n enginestatus: {Engine_status},\n workinghours: {Working_Hours},\n batterylevel: {Battery_Level},\n devicetampering: {Device_Tampering},\n enginerpm: {Engine_RPM},\n longitude: {x},\n latitude: {y}")

		url= 'https://192.168.10.17/server/rest/services/IOT_1/FeatureServer/0/addFeatures/'
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
		sensor_data['longitude'] = str(location.split(",")[0]),
		sensor_data['latitude'] = str(location.split(",")[1])
		
		Sensor_json_data = json.dumps(sensor_data)

		print ("Publishing fake IOT Values: ", str(id), str(device_id), str(Sensor_time), str(Engine_status), str(Working_Hours), str(Battery_Level), str(Device_Tampering), str(Engine_RPM), x, y)
		# publish_To_Topic (MQTT_Topic_IOT_Data, Sensor_json_data)
		
		#Push into DB Table
		insert_script = ("insert into public.iot_data_2 (deviceid, DateTime, EngineStatus, WorkingHours, BatteryLevel, DeviceTampering, EngineRPM, longitude, latitude) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)")
		insert_value = (device_id, Sensor_time, Engine_status, Working_Hours, Battery_Level, Device_Tampering, Engine_RPM, x, y)
		curs.execute(insert_script, insert_value)
		# curs.execute("UPDATE sde.iot_data2 SET shape = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);")
		conn.commit()
		print ("Inserted Fake_IOT_Data into Database.")
		print ("")
		
		time.sleep(60)

publish_Fake_Sensor_Values_to_MQTT()

#====================================================
