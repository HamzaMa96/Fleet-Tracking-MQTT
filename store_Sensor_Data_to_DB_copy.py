import json
import psycopg2
from shapely.geometry import Point
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
def IOT_Data(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	device_id = json_Dict['deviceid']
	Data_and_Time = json_Dict['DateTime']
	Engine_Status = json_Dict['EngineStatus']
	Working_Hours = json_Dict['WorkingHours']
	Battery_Level =	json_Dict['BatteryLevel']
	Device_Tampering =	json_Dict['DeviceTampering']
	Engine_RPM = json_Dict['EngineRPM']
	x = json_Dict['longitude']
	y = json_Dict['latitude']
	# pt = Point(y,x)
	
	#Push into DB Table
	insert_script = ("insert into public.iot_data_2 (deviceid, DateTime, EngineStatus, WorkingHours, BatteryLevel, DeviceTampering, EngineRPM, longitude, latitude) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)")
	insert_value = (device_id, Data_and_Time, Engine_Status, Working_Hours, Battery_Level, Device_Tampering, Engine_RPM, x, y)
	curs.execute(insert_script, insert_value)
	# curs.execute("UPDATE sde.iot_data2 SET shape = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);")
	conn.commit()
	print ("Inserted Fake_IOT_Data into Database.")
	print ("")
#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler_1(Topic, jsonData):
	if Topic == "IOT/Vehicle/Data":
		IOT_Data(jsonData)
#===============================================================
