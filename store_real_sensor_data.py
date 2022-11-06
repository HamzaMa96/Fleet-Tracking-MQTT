import json
import psycopg2
import urllib
from datetime import datetime, date, time, timedelta
import requests

# postgres DB credentials
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

# Function to save Engine Status to DB Table
def real_data(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	Device_IMEI = json_Dict['00']
	Time_Stamp = json_Dict['01']
	Firmware_Version = json_Dict['04']
	Device_Uptime = json_Dict['06']
	Built_In_Battery_Level = json_Dict['07']
	Device_Tampering = json_Dict['08']
	External_Power_Source_Status = json_Dict['09']
	Engine_Temperature = json_Dict['13']
	Engine_Speed_RPM = json_Dict['14']
	Coolant_Level = json_Dict['15']
	GPS_Location = json_Dict['22']

	features=[
				{
					"attributes":{
					"00": str(Device_IMEI),
					"01": str(Time_Stamp),
					"04": str(Firmware_Version),
					"06": str(Device_Uptime),
					"07": str(Built_In_Battery_Level),
					"09": str(External_Power_Source_Status),
					"13": str(Engine_Temperature),
					"14": str(Engine_Speed_RPM),
					"15": str(Coolant_Level),
					"22": str(GPS_Location)
					},
					"geometry" : {
					"x" : 
					"y" : 
					}
				}
			]

	url= 'https://192.168.10.17/server/rest/services/iot/FeatureServer/0/addFeatures/'
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	params = urllib.parse.urlencode({'f': 'json', 'features': json.dumps(features)})
	r = requests.post(url, verify=False ,data= params, headers= headers)
	print(f"{r.text}")

	sensor_data= {}
	sensor_data['00'] = Device_IMEI
	sensor_data['01'] = Time_Stamp
	sensor_data['04'] = Firmware_Version
	sensor_data['06'] = Device_Uptime
	sensor_data['07'] = Built_In_Battery_Level
	sensor_data['08'] = Device_Tampering
	sensor_data['09'] = External_Power_Source_Status
	sensor_data['13'] = Engine_Temperature
	sensor_data['14'] = Engine_Speed_RPM
	sensor_data['15'] = Coolant_Level
	sensor_data['22'] = GPS_Location

	# print ("Publishing fake IOT Values: ", str(id), str(device_id), str(Sensor_time), str(Engine_status), str(Working_Hours), str(Battery_Level), str(Device_Tampering), str(Engine_RPM), x, y)
	# publish_To_Topic (MQTT_Topic_IOT_Data, Sensor_json_data)
	
	#Push into DB Table
	insert_script = ("insert into public.iot_data_real (00, 01, 04, 06, 07, 08, 09, 13, 14, 15, 22) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
	insert_value = (Device_IMEI, Time_Stamp, Firmware_Version, Device_Uptime, Built_In_Battery_Level, Device_Tampering, External_Power_Source_Status, Engine_Temperature, Engine_Speed_RPM, Coolant_Level, GPS_Location)
	curs.execute(insert_script, insert_value)
	conn.commit()
	print ("Inserted IOT_Data into Database.")
	print ("")
	# time.sleep(60)
#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
	if Topic == "#":
		real_data(jsonData)
	else:
		real_data(jsonData)
#===============================================================
