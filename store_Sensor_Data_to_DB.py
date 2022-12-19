import json
import psycopg2

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
def Engine_Status(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	Data_and_Time = json_Dict['Date']
	Engine_Status = json_Dict['Engine_Status']
	
	#Push into DB Table
	insert_script = ("insert into Engine_Status (Date_n_Time, Engine_Status) values (%s,%s)")
	insert_value = (Data_and_Time, Engine_Status)
	curs.execute(insert_script, insert_value)
	conn.commit()
	print ("Inserted Engine Status Data into Database.")
	print ("")

# Function to save Working Hours to DB Table
def Working_Hours(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	Data_and_Time = json_Dict['Date']
	WorkingHours = json_Dict['WorkingHours']
	
	#Push into DB Table
	insert_script = ("insert into Working_Hours (Date_n_Time, WorkingHours) values (%s, %s)")
	insert_value = (Data_and_Time, WorkingHours)
	curs.execute(insert_script, insert_value)
	conn.commit()
	print ("Inserted Working Hours Data into Database.")
	print ("")

# Function to save Car Battery Level to DB Table
def Battery_Level(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	Data_and_Time = json_Dict['Date']
	BatteryLevel = json_Dict['BatteryLevel']
	
	#Push into DB Table
	insert_script = ("insert into Battery_Level (Date_n_Time, BatteryLevel) values (%s, %s)")
	insert_value = (Data_and_Time, BatteryLevel)
	curs.execute(insert_script, insert_value)
	conn.commit()
	print ("Inserted BatteryLevel Data into Database.")
	print ("")

# Function to save Device Tampering to DB Table
def Device_Tampering(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	Data_and_Time = json_Dict['Date']
	Device_Tampering = json_Dict['DeviceTampering']
	
	#Push into DB Table
	insert_script = ("insert into Device_Tampering (Date_n_Time, DeviceTampering) values (%s, %s)")
	insert_value = (Data_and_Time, Device_Tampering)
	curs.execute(insert_script, insert_value)
	conn.commit()
	print ("Inserted Device Tampering Data into Database.")
	print ("")

# Function to save Device Tampering to DB Table
def Engine_RPM(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	Data_and_Time = json_Dict['Date']
	Engine_RPM = json_Dict['EngineRPM']
	
	#Push into DB Table
	insert_script = ("insert into Engine_RPM (Date_n_Time, EngineRPM) values (%s, %s)")
	insert_value = (Data_and_Time, Engine_RPM)
	curs.execute(insert_script, insert_value)
	conn.commit()
	print ("Inserted Engine_RPM Data into Database.")
	print ("")

# Function to save Location to DB Table
def location(jsonData):
	#Parse Data 
	json_Dict = json.loads(jsonData)
	Data_and_Time = json_Dict['Date']
	x = json_Dict['longitude']
	y = json_Dict['latitude']
	
	#Push into DB Table
	insert_script = ("insert into location (Date_n_Time, x, y) values (%s,%s,%s)")
	insert_value = (Data_and_Time, x, y)
	curs.execute(insert_script, insert_value)
	conn.commit()
	print ("Inserted location Status Data into Database.")
	print ("")
#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
	if Topic == "IOT/Vehicle/EngineStatus":
		Engine_Status(jsonData)
	elif Topic == "IOT/Vehicle/WorkingHours":
		Working_Hours(jsonData)
	elif Topic == "IOT/Vehicle/BatteryLevel":
		Battery_Level(jsonData)
	elif Topic == "IOT/Vehicle/DeviceTampering":
		Device_Tampering(jsonData)		
	elif Topic == "IOT/Vehicle/EngineRPM":
		Engine_RPM(jsonData)
	elif Topic == "IOT/Vehicle/location":
		location(jsonData)
#===============================================================
