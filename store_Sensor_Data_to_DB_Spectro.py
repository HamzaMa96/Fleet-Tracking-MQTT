import json
import psycopg2
from shapely.geometry import Point
import urllib
import requests
import numpy as np
import pandas as pd
import time
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
	if not(json_Dict.get("16") is None):
		Working_Hours = json_Dict['16']
	else:
		Working_Hours = np.nan
	if not(json_Dict.get("06") is None):
		DeviceUpTime = json_Dict['06']
	else:
		DeviceUpTime = np.nan
	if not(json_Dict.get("01") is None):
		TimeStamp = json_Dict['01']
		date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(TimeStamp))
	else:
		TimeStamp = np.nan
	if not(json_Dict.get("07") is None):
		BuiltInBattery = json_Dict['07']
	else:
		BuiltInBattery = np.nan
	if not(json_Dict.get("09") is None):
		ExternalPowerSource = json_Dict['09']
	else:
		ExternalPowerSource = np.nan
	if not(json_Dict.get("25") is None):
		CellularRSSI = json_Dict['25']
	else:
		CellularRSSI = np.nan
	if not(json_Dict.get("00") is None):
		IMEI = json_Dict['00']
	else:
		IMEI = np.nan
	if not(json_Dict.get("22") is None):
		GPS_Location = json_Dict['22']
		x = GPS_Location.split(',')[0]
		y = GPS_Location.split(',')[1]
	else:
		GPS_Location = np.nan
		x = np.nan
		y = np.nan
	if not(json_Dict.get("13") is None):
		EngineTemp = json_Dict['13']
	else:
		EngineTemp = np.nan
	if not(json_Dict.get("14") is None):
		EngineRPM = json_Dict['14']
	else:
		EngineRPM = np.nan
	if not(json_Dict.get("30") is None):
		CarBatteryLevel = json_Dict['30']
	else:
		CarBatteryLevel = np.nan
	if not(json_Dict.get("15") is None):
		CoolantLevel = json_Dict['15']
	else:
		CoolantLevel = np.nan
	if not(json_Dict.get("17") is None):
		FuelConsumption = json_Dict['17']
	else:
		FuelConsumption = np.nan
	if not(json_Dict.get("18") is None):
		FuelTankLevel = json_Dict['18']
	else:
		FuelTankLevel = np.nan
	if not(json_Dict.get("20") is None):
		CarSpeed = json_Dict['20']
	else:
		CarSpeed = np.nan
	if not(json_Dict.get("23") is None):
		VehicleDistance = json_Dict['23']
	else:
		VehicleDistance = np.nan
	if not(json_Dict.get("28") is None):
		MAC = json_Dict['28']
	else:
		MAC = np.nan
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
					"datetime": str(date),
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
					"x": x,
					"y": y,
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

	url= 'https://192.168.10.17/server/rest/services/Spectro/FeatureServer/0/addFeatures/'
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
	print(date)
	print ("")
	
#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler_Spectro(Topic, jsonData):
	if Topic == "IOT/Vehicle/Data":
		IOT_Data(jsonData)
#===============================================================
