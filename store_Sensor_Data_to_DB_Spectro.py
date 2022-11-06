import json
import psycopg2
from shapely.geometry import Point
import urllib
import requests
import numpy as np
import pandas as pd
from datetime import datetime, date, time, timedelta
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
	if not(json_Dict.get("29") is None):
		TotalOperationHour = json_Dict['29']
	else:
		TotalOperationHour = np.nan
# elif "16" in json_Dict:
	Working_Hours = json_Dict['16']
# elif "06" in json_Dict:
	DeviceUpTime = json_Dict['06']
# elif "01" in json_Dict:
	TimeStamp = json_Dict['01']
# elif "07" in json_Dict:
	BuiltInBattery = json_Dict['07']
# elif "09" in json_Dict:
	ExternalPowerSource = json_Dict['09']
# elif "25" in json_Dict:
	CellularRSSI = json_Dict['25']
# elif "00" in json_Dict:
	IMEI = json_Dict['00']
# elif "22" in json_Dict:
	GPS_Location = json_Dict['22']
	x = GPS_Location.split(',')[0]
	y = GPS_Location.split(',')[1]
# elif "13" in json_Dict:
	EngineTemp = json_Dict['13']
# elif "14" in json_Dict:
	EngineRPM = json_Dict['14']
# elif "30" in json_Dict:
	CarBatteryLevel = json_Dict['30']
# elif "15" in json_Dict:
	CoolantLevel = json_Dict['15']
# elif "17" in json_Dict:
	FuelConsumption = json_Dict['17']
# elif "18" in json_Dict:
	FuelTankLevel = json_Dict['18']
# elif "20" in json_Dict:
	CarSpeed = json_Dict['20']
# elif "23" in json_Dict:
	VehicleDistance = json_Dict['23']
# elif "28" in json_Dict:
	MAC = json_Dict['28']
	if not(json_Dict.get("04") is None):
		Firmware = json_Dict['04']
	else:
		Firmware = np.nan
	if not(json_Dict.get("08") is None):
		DeviceTampering = json_Dict['08']
	else:
		DeviceTampering = np.nan
	if not(json_Dict.get("27") is None):
		IMSI = json_Dict['27']
	else:
		IMSI = np.nan
	if not(json_Dict.get("35") is None):
		FuelRate = json_Dict['35']
	else:
		FuelRate = np.nan

	#Push into ArcGIS DB
	features=[
				{
					"attributes":{
					"IMEI": str(IMEI),
					"datetime": str(pd.to_datetime(TimeStamp)),
					"FirmWare": str(Firmware),
					"UpTime": str(DeviceUpTime),
					"BuiltInBattery": str(BuiltInBattery),
					"Tampering": str(DeviceTampering),
					"ExternalPower": str(ExternalPowerSource),
					"EngineTemperature": str(EngineTemp),
					"RPM": str(EngineRPM),
					"CoolantLevel": str(CoolantLevel),
					"FuelConsumption": str(FuelConsumption),
					"FuelTankLevel": str(FuelTankLevel),
					"Speed": str(CarSpeed),
					"location": str(GPS_Location),
					"longitude": str(x),
					"latitude": str(y),
					"VehicleDistance": str(VehicleDistance),
					"CellularRSSI": str(CellularRSSI),
					"IMSI": str(IMSI),
					"MAC": str(MAC),
					"TotalOperationHour": str(TotalOperationHour),
					"CarBatteryLevel": str(CarBatteryLevel),
					"FuelRate": str(FuelRate)
					},
					"geometry" : {
					"x" : x,
					"y" : y
					}
				}
			]

	url= 'https://192.168.10.17/server/rest/services/Spectro_IOT_Data/FeatureServer/0/addFeatures/'
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	params = urllib.parse.urlencode({'f': 'json', 'features': json.dumps(features)})
	r = requests.post(url, verify=False ,data= params, headers= headers)
	print(f"{r.text}")

	#Push into DB Table
	insert_script = ("insert into public.iot_data_real (TotalOperationHour, WorkingHours, UpTime, DateTime, BuiltInBattery, ExternalPower, CellularRSSI, IMEI, Location, x, y, EngineTemperature, RPM, BatteryLevel, CoolantLevel, FuelConsumption, FuelTankLevel, Speed, VehicleDistance, MAC, Firmware, Tampering, IMSI, FuelRate) values (%s,%s,%s,to_timestamp(%s),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
	insert_value = (TotalOperationHour, Working_Hours, DeviceUpTime, TimeStamp, BuiltInBattery, ExternalPowerSource, CellularRSSI, IMEI, GPS_Location, x, y, EngineTemp, EngineRPM, CarBatteryLevel, CoolantLevel, FuelConsumption, FuelTankLevel, CarSpeed, VehicleDistance, MAC, Firmware, DeviceTampering, IMSI, FuelRate)
	curs.execute(insert_script, insert_value)
	# curs.execute("UPDATE sde.iot_data2 SET shape = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);")
	conn.commit()
	print ("Inserted Spectro_IOT_Data into Database.")
	print(TimeStamp)
	print ("")
	
#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler_1(Topic, jsonData):
	if Topic == "IOT/Vehicle/Data":
		IOT_Data(jsonData)
#===============================================================
