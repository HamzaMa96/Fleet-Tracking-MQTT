import json
# import psycopg2
import pyodbc
from requests import Session
# SQL Server DB credentials


# Function to save Engine Status to DB Table
def IOT_Data(jsonData):
	while True:
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
		session = Session()
		session.head('https://192.168.10.17/server/rest/services/Vehicles/FeatureServer/0/', verify=False)

		session.post(
			url='https://192.168.10.17/server/rest/services/Vehicles/FeatureServer/0/addFeatures/',
			features=[
				{
					"attributes":
					{
						
						"deviceid": "{device_id}",
						"datetime": "{Data_and_Time}",
						"enginestatus": "{Engine_Status}",
						"workinghours": "{Working_Hours}",
						"batterylevel": "{Battery_Level}",
						"devicetampering": "{Device_Tampering}",
						"enginerpm": "{Engine_RPM}",
						"longitude": "{x}",
						"latitude": "{y}"
						},
						"geometry" : {
						"x" : "{x}",
						"y" : "{y}"
					}
				}
			]
		)
	#===============================================================
	# Master Function to Select DB Funtion based on MQTT Topic
	#===============================================================
