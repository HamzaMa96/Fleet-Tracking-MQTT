import psycopg2
import pyodbc
# SQLite DB Name
# DB_Name =  "mqtt"
# hostname = 'localhost'
# username = 'postgres'
# pws = 'password@1324'
# port_id = 5432

# SQLite DB Table Schema
# TableSchema="""
# drop table if exists DHT22_Temperature_Data ;
# drop table if exists DHT22_Humidity_Data ;

# create table Engine_Status (
#   id serial primary key,
#   Date_n_Time timestamp,
#   Engine_Status int
# );

# create table Working_Hours (
#   id serial primary key,
#   Date_n_Time timestamp,
#   WorkingHours float
# );

# create table Battery_Level (
#   id serial primary key,
#   Date_n_Time timestamp,
#   BatteryLevel int
# );

# create table Device_Tampering (
#   id serial primary key,
#   Date_n_Time timestamp,
#   DeviceTampering int
# );

# create table Engine_RPM (
#   id serial primary key,
#   Date_n_Time timestamp,
#   EngineRPM float
# );

# create table location (
#   id serial primary key,
#   Date_n_Time timestamp,
#   x float,
#   y float
# );
# """

TableSchema="""
create table IOT_Data_Spectro_3 (
  device_IMEI varchar(150),
  timestamp datetime,
  firmware_version varchar(150),
  device_uptime bigint,
  builtinBattery float,
  device_tampering int,
  externalPowerSource int,
  engine_temp int,
  RPM int,
  coolane_level int,
  fuel_consumption float,
  fuel_tank_level float,
  car_speed int,
  GPS varchar(150),
  vehicle_distance int,
  cellular_rssi varchar(150),
  IMSI varchar(150),
  MAC varchar(150),
  total_operation_hour int,
  car_battery_level varchar(150),
  fuel_rate varchar(150),
  x float,
  y float
);
"""

#Connect or Create DB File
# conn = psycopg2.connect(
#         host = hostname,
#         dbname = DB_Name,
#         user = username,
#         password = pws,
#         port = port_id
#   )

conn= pyodbc.connect('driver={SQL Server}; server=WIN-RD5LI81CPFJ; database=GISDB; UID=sde; PWD=Test@1234;')
curs = conn.cursor()

#Create Tables
curs.execute(TableSchema)
conn.commit()

#Close DB
curs.close()
conn.close()
