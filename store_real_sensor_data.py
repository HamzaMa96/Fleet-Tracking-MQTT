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
	if "00" in json_Dict:
		Device_IMEI = json_Dict['00']
	elif "01" in json_Dict:
		Time_Stamp = json_Dict['01']
	elif "04" in json_Dict:
		Firmware_Version = json_Dict['04']
	elif "06" in json_Dict:
		Device_Uptime = json_Dict['06']
	elif "07" in json_Dict:
		Built_In_Battery_Level = json_Dict['07']
	elif "08" in json_Dict:
		Device_Tampering = json_Dict['08']
	elif "09" in json_Dict:
		External_Power_Source_Status = json_Dict['09']
	elif "13" in json_Dict:
		Engine_Temperature = json_Dict['13']
	elif "14" in json_Dict:
		Engine_Speed_RPM = json_Dict['14']
	elif "15" in json_Dict:
		Coolant_Level = json_Dict['15']
	elif "17" in json_Dict:
		Fuel_Consumption = json_Dict['17']
	elif "18" in json_Dict:
		Fuel_Tank_Level = json_Dict['18']
	elif "20" in json_Dict:
		CAR_Speed = json_Dict['20']
	elif "22" in json_Dict:
		GPS_Location = json_Dict['22']
		x = GPS_Location.split(' ')[0]
		y = GPS_Location.split(' ')[1]
	elif "23" in json_Dict:
		Vehicle_Distance = json_Dict['23']
	elif "25" in json_Dict:
		Cellular_RSSI = json_Dict['25']
	elif "27" in json_Dict:
		IMSI = json_Dict['27']
	elif "28" in json_Dict:
		MAC = json_Dict['28']
	elif "29" in json_Dict:
		Total_Operation_Hour = json_Dict['29']
	elif "30" in json_Dict:
		Car_Battery_Level = json_Dict['30']
	elif "35" in json_Dict:
		Fuel_Rate = json_Dict['35']

	features=[
				{
					"attributes":{
					"00": str(Device_IMEI),
					"01": str(Time_Stamp),
					"04": str(Firmware_Version),
					"06": str(Device_Uptime),
					"07": str(Built_In_Battery_Level),
					"08": str(Device_Tampering),
					"09": str(External_Power_Source_Status),
					"13": str(Engine_Temperature),
					"14": str(Engine_Speed_RPM),
					"15": str(Coolant_Level),
					"17": str(Fuel_Consumption),
					"18": str(Fuel_Tank_Level),
					"20": str(CAR_Speed),
					"22": str(GPS_Location),
					"23": str(Vehicle_Distance),
					"25": str(Cellular_RSSI),
					"27": str(IMSI),
					"28": str(MAC),
					"29": str(Total_Operation_Hour),
					"30": str(Car_Battery_Level),
					"35": str(Fuel_Rate),
					},
					"geometry" : {
					"x" : x,
					"y" : y
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
	sensor_data['17'] = Fuel_Consumption
	sensor_data['18'] = Fuel_Tank_Level
	sensor_data['20'] = CAR_Speed
	sensor_data['22'] = GPS_Location
	sensor_data['x'] = x
	sensor_data['y'] = y
	sensor_data['23'] = Vehicle_Distance
	sensor_data['25'] = Cellular_RSSI
	sensor_data['27'] = IMSI
	sensor_data['28'] = MAC
	sensor_data['29'] = Total_Operation_Hour
	sensor_data['30'] = Car_Battery_Level
	sensor_data['35'] = Fuel_Rate

	# print ("Publishing fake IOT Values: ", str(id), str(device_id), str(Sensor_time), str(Engine_status), str(Working_Hours), str(Battery_Level), str(Device_Tampering), str(Engine_RPM), x, y)
	# publish_To_Topic (MQTT_Topic_IOT_Data, Sensor_json_data)
	
	#Push into DB Table
	insert_script = ("insert into public.iot_data_2 (00, 01, 04, 06, 07, 08, 09, 13, 14, 15, 18, 20, 22, x, y, 23, 25, 27, 28, 29, 30, 35) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
	insert_value = (Device_IMEI, Time_Stamp, Firmware_Version, Device_Uptime, Built_In_Battery_Level, Device_Tampering, External_Power_Source_Status, Engine_Temperature, Engine_Speed_RPM, Coolant_Level, Fuel_Consumption, Fuel_Tank_Level, CAR_Speed, GPS_Location, x, y, Vehicle_Distance, Cellular_RSSI, IMSI, MAC, Total_Operation_Hour, Car_Battery_Level, Fuel_Rate)
	curs.execute(insert_script, insert_value)
	conn.commit()
	print ("Inserted IOT_Data into Database.")
	print ("")
	time.sleep(60)
#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
	if Topic == "IOT/Vehicle/Data":
		real_data(jsonData)
#===============================================================
