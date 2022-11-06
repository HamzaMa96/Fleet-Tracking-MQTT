import json
# import psycopg2
import pyodbc
from requests import Session
# SQL Server DB credentials


# Function to save Engine Status to DB Table
def IOT_Data(jsonData):
	conn= pyodbc.connect('driver={SQL Server}; server=ARCGISDB; database=TrackingDB; UID=sa; PWD=Pa$$w0rd;')

	curs = conn.cursor()
	#Parse Data 
	json_Dict = json.loads(jsonData)
	object_id = json_Dict['objectid']
	device_id = json_Dict['deviceid']
	Data_and_Time = json_Dict['datetime']
	Engine_Status = json_Dict['enginestatus']
	Working_Hours = json_Dict['workinghours']
	Battery_Level =	json_Dict['batterylevel']
	Device_Tampering =	json_Dict['devicetampering']
	Engine_RPM = json_Dict['enginerpm']
	x = json_Dict['longitude']
	y = json_Dict['latitude']
	
	#Push into DB Table
	# insert_script = ("insert into dbo.IOT_DATA2 (objectid, deviceid, DateTime, EngineStatus, WorkingHours, BatteryLevel, DeviceTampering, EngineRPM, longitude, latitude) values (?,?,?,?,?,?,?,?,?,?)")
	# insert_value = (object_id, device_id, Data_and_Time, Engine_Status, Working_Hours, Battery_Level, Device_Tampering, Engine_RPM, x, y)
	# curs.execute(insert_script, insert_value)
	# conn.commit()
	# print ("Inserted Fake_IOT_Data into Database.")
	# print ("")
	# conn.close()
	
	session = Session()
	session.head('https://192.168.10.17/server/rest/services/Vehicles/FeatureServer/0/', verify=False)

	session.post(
		url='https://192.168.10.17/server/rest/services/Vehicles/FeatureServer/0/addFeatures/',
		features=[
			{
			"attributes":
				{
					"objectid": f"{object_id}",
					"deviceid": f"{device_id}",
					"datetime": f"{Data_and_Time}",
					"enginestatus": f"{Engine_Status}",
					"workinghours": f"{Working_Hours}",
					"batterylevel": f"{Battery_Level}",
					"devicetampering": f"{Device_Tampering}",
					"enginerpm": f"{Engine_RPM}",
					"longitude": f"{x}",
					"latitude": f"{y}"
					},
					"geometry" : {
					"x" : f"{x}",
					"y" : f"{y}"
				}
			}
		]
	)
#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler_1(Topic, jsonData):
	if Topic == "IOT/Vehicle/Data":
		IOT_Data(jsonData)
#===============================================================
