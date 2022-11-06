import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime

#====================================================
# MQTT Settings 
MQTT_Broker = "127.0.0.1"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic_Engine_Status = "IOT/Vehicle/EngineStatus"
MQTT_Topic_Working_Hours = "IOT/Vehicle/WorkingHours"
MQTT_Topic_Bettery_Level = "IOT/Vehicle/BatteryLevel"
MQTT_Topic_Device_Tampering = "IOT/Vehicle/DeviceTampering"
MQTT_Topic_Engine_RPM = "IOT/Vehicle/EngineRPM"
MQTT_Topic_location = "IOT/Vehicle/location"
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

toggle = 0

def publish_Fake_Sensor_Values_to_MQTT():
	threading.Timer(3.0, publish_Fake_Sensor_Values_to_MQTT).start()
	global toggle
	if toggle == 0:
		Engine_status = int(random.uniform(0, 2))

		engine_data = {}
		engine_data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
		engine_data['Engine_Status'] = Engine_status
		Engine_json_data = json.dumps(engine_data)

		print ("Publishing fake Engine Status Value: " , str(Engine_status) , "...")
		publish_To_Topic (MQTT_Topic_Engine_Status, Engine_json_data)
		toggle = 1

	elif toggle ==1:
		Working_Hours = float("{0:.2f}".format(random.uniform(0, 24)))

		Working_Data = {}
		Working_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
		Working_Data['WorkingHours'] = Working_Hours
		Working_json_data = json.dumps(Working_Data)

		print ("Publishing fake Working Hours Value: " , str(Working_Hours) , "...")
		publish_To_Topic (MQTT_Topic_Working_Hours, Working_json_data)
		toggle = 2

	elif toggle ==2:
		Battery_Level = int(random.uniform(25, 100))

		Battery_Data = {}
		Battery_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
		Battery_Data['BatteryLevel'] = Battery_Level
		Battery_json_data = json.dumps(Battery_Data)

		print ("Publishing fake BatteryLevel Value: " , str(Battery_Level) , "...")
		publish_To_Topic (MQTT_Topic_Bettery_Level, Battery_json_data)
		toggle = 3

	elif toggle ==3:
		Device_Tampering = int(random.uniform(0, 2))

		Tampering_data = {}
		Tampering_data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
		Tampering_data['DeviceTampering'] = Device_Tampering
		Tampering_json_data = json.dumps(Tampering_data)

		print ("Publishing fake Device Tampering Value: " , str(Device_Tampering) , "...")
		publish_To_Topic (MQTT_Topic_Device_Tampering, Tampering_json_data)
		toggle = 4

	elif toggle ==4:
		Engine_RPM = float("{0:.1f}".format(random.uniform(0, 6)))

		RPM_Data = {}
		RPM_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
		RPM_Data['EngineRPM'] = Engine_RPM
		RPM_json_data = json.dumps(RPM_Data)

		print ("Publishing fake Engine RPM Value: " , str(Engine_RPM) , "...")
		publish_To_Topic (MQTT_Topic_Engine_RPM, RPM_json_data)
		toggle = 5

	elif toggle ==5:
		x = float("{0:.1f}".format(random.uniform(0, 6)))
		y = float("{0:.1f}".format(random.uniform(0, 6)))

		location_data = {}
		location_data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S")
		location_data['longitude'] = x
		location_data['latitude'] = y
		location_json_data = json.dumps(RPM_Data)

		print ("Publishing fake Engine RPM Value: " , str(x) , str(y) ,  "...")
		publish_To_Topic (MQTT_Topic_Engine_RPM, location_json_data)
		toggle = 0

publish_Fake_Sensor_Values_to_MQTT()

#====================================================
