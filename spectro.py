import requests
import numpy as np
import pandas as pd
from datetime import datetime, date, time, timedelta
import time
import json
import psycopg2
import urllib
import pyodbc
# postgres DB credentials
# DB_Name =  "mqtt"
# hostname = 'localhost'
# username = 'postgres'
# pws = 'password@1324'
# port_id = 5432
# curs = None
# conn = None
#===============================================================
# Connection initialization
# conn= psycopg2.connect(
# 			host = hostname,
# 			dbname = DB_Name,
# 			user = username,
# 			password = pws,
# 			port = port_id)
# conn= pyodbc.connect('driver={SQL Server}; server=WIN-RD5LI81CPFJ; database=GISDB; UID=sde; PWD=Test@1234;')
# curs = conn.cursor()
# Function to save Engine Status to DB Table
def IOT_Data(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	if not(json_Dict.get("29") is None):
		TotalOperationHour = json_Dict['29']
		print(TotalOperationHour)
	else:
		TotalOperationHour = 0
	if not(json_Dict.get("16") is None):
		Working_Hours = json_Dict['16']
	else:
		Working_Hours = 0
	if not(json_Dict.get("06") is None):
		DeviceUpTime = json_Dict['06']
	else:
		DeviceUpTime = 0
	if not(json_Dict.get("01") is None):
		TimeStamp = json_Dict['01']
		# time = pd.to_datetime(TimeStamp)
		TimeStamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(TimeStamp))
	else:
		TimeStamp = datetime.now()
	if not(json_Dict.get("07") is None):
		BuiltInBattery = json_Dict['07']
	else:
		BuiltInBattery = 0
	if not(json_Dict.get("09") is None):
		ExternalPowerSource = json_Dict['09']
	else:
		ExternalPowerSource = 0
	if not(json_Dict.get("25") is None):
		CellularRSSI = json_Dict['25']
	else:
		CellularRSSI = 0
	if not(json_Dict.get("00") is None):
		IMEI = json_Dict['00']
	else:
		IMEI = 0
	if not(json_Dict.get("22") is None):
		GPS_Location = json_Dict['22']
		x = GPS_Location.split(',')[0]
		y = GPS_Location.split(',')[1]
	else:
		GPS_Location = 0
		x = 0
		y = 0
	if not(json_Dict.get("13") is None):
		EngineTemp = json_Dict['13']
	else:
		EngineTemp = 0
	if not(json_Dict.get("14") is None):
		EngineRPM = json_Dict['14']
	else:
		EngineRPM = 0
	if not(json_Dict.get("30") is None):
		CarBatteryLevel = json_Dict['30']
	else:
		CarBatteryLevel = 0
	if not(json_Dict.get("15") is None):
		CoolantLevel = json_Dict['15']
	else:
		CoolantLevel = 0
	if not(json_Dict.get("17") is None):
		FuelConsumption = json_Dict['17']
	else:
		FuelConsumption = 0
	if not(json_Dict.get("18") is None):
		FuelTankLevel = json_Dict['18']
	else:
		FuelTankLevel = 0
	if not(json_Dict.get("20") is None):
		CarSpeed = json_Dict['20']
	else:
		CarSpeed = 0
	if not(json_Dict.get("23") is None):
		VehicleDistance = json_Dict['23']
	else:
		VehicleDistance = 0
	if not(json_Dict.get("28") is None):
		MAC = json_Dict['28']
	else:
		MAC = 0
	if not(json_Dict.get("04") is None):
		Firmware = json_Dict['04']
	else:
		Firmware = 0
	if not(json_Dict.get("08") is None):
		DeviceTampering = json_Dict['08']
	else:
		DeviceTampering = 0
	if not(json_Dict.get("27") is None):
		IMSI = json_Dict['27']
	else:
		IMSI = 0
	if not(json_Dict.get("35") is None):
		FuelRate = json_Dict['35']
	else:
		FuelRate = 0
	#Push into ArcGIS DB
	features=[
				{
					"attributes":{
					"device_IMEI": str(IMEI),
					"timestamp": str(TimeStamp),
					"firmware_version": str(Firmware),
					"device_uptime": str(DeviceUpTime),
					"builtinBattery": str(BuiltInBattery),
					"device_tampering": str(DeviceTampering),
					"externalPowerSource": str(ExternalPowerSource),
					"engine_temp": str(EngineTemp),
					"RPM": str(EngineRPM),
					"coolane_level": str(CoolantLevel),
					"fuel_consumption": str(FuelConsumption),
					"fuel_tank_level": str(FuelTankLevel),
					"car_speed": str(CarSpeed),
					"GPS": str(GPS_Location),
					"x": str(x),
					"y": str(y),
					"vehicle_distance": str(VehicleDistance),
					"cellular_rssi": str(CellularRSSI),
					"IMSI": str(IMSI),
					"MAC": str(MAC),
					"total_operation_hour": str(TotalOperationHour),
					"car_battery_level": str(CarBatteryLevel),
					"fuel_rate": str(FuelRate)
					},
					"geometry" : {
					"x" : x,
					"y" : y
					}
				}
			]

	print(TotalOperationHour, Working_Hours, DeviceUpTime, TimeStamp, BuiltInBattery, ExternalPowerSource, CellularRSSI, IMEI, GPS_Location, x, y, EngineTemp, EngineRPM, CarBatteryLevel, CoolantLevel, FuelConsumption, FuelTankLevel, CarSpeed, VehicleDistance, MAC, Firmware, DeviceTampering, IMSI, FuelRate, "x:",x, "y:",y)
	
	url= 'https://win-rd5li81cpfj.itechs-poc.com/Server/rest/services/Spectro/FeatureServer/0/addFeatures'
	headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
	params = urllib.parse.urlencode({'f': 'json', 'features': json.dumps(features)})
	r = requests.post(url, verify=False ,data= params, headers= headers)
	print(f"{r.text}")
	# Push into DB Table
	# insert_script = ("insert into dbo.IOT_Data_Spectro_2 (total_operation_hour, device_uptime, timestamp, builtinBattery, externalPowerSource, cellular_rssi, device_IMEI, GPS, engine_temp, RPM, car_battery_level, coolane_level, fuel_consumption, fuel_tank_level, car_speed, vehicle_distance, MAC, firmware_version, device_tampering, IMSI, fuel_rate) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	# insert_value = (TotalOperationHour, DeviceUpTime, TimeStamp, BuiltInBattery, ExternalPowerSource, CellularRSSI, IMEI, GPS_Location, EngineTemp, EngineRPM, CarBatteryLevel, CoolantLevel, FuelConsumption, FuelTankLevel, CarSpeed, VehicleDistance, MAC, Firmware, DeviceTampering, IMSI, FuelRate)
	# curs.execute(insert_script, insert_value)
	# curs.execute("UPDATE sde.iot_data2 SET shape = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);")
	# conn.commit()
	print ("Inserted Spectro_IOT_Data into Database.")
	print(TimeStamp)
	print ("")

#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler_Spectro(Topic, jsonData):
	if Topic == "IOT/Vehicle/Data":
		IOT_Data(jsonData)
#===============================================================