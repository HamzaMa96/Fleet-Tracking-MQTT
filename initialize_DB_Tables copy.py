import psycopg2
import pyodbc
# Postgres DB Name
DB_Name =  "mqtt"
hostname = 'localhost'
username = 'postgres'
pws = 'password@1324'
port_id = 5432
# SQL Server DB credentials
# conn= pyodbc.connect('driver={SQL Server}; server=ARCGISDB; database=TrackingDB; UID=sa; PWD=Pa$$w0rd;')
# SQL Server Table Schema
TableSchema="""
drop table if exists IOT_Data_real ;

create table IOT_Data_Spectro (
  id serial primary key,
  IMEI bigint,
  DateTime timestamp,
  Firmware varchar(50),
  UpTime int,
  BuiltInBattery float,
  Tampering varchar(50),
  ExternalPower int,
  EngineTemperature int,
  RPM int,
  CoolantLevel int,
  WorkingHours int,
  FuelConsumption float,
  FuelTankLevel float,
  Speed int,
  Location varchar(50),
  x float,
  y float,
  VehicleDistance int,
  CellularRSSI varchar(50),
  IMSI varchar(50),
  MAC varchar(50),
  TotalOperationHour int,
  BatteryLevel float,
  FuelRate varchar(50)

);
"""

#Connect or Create DB File
# conn= pyodbc.connect('driver={SQL Server}; server=ARCGISDB; database=TrackingDB; UID=sa; PWD=Pa$$w0rd;')
# curs = conn.cursor()
conn = psycopg2.connect(
        host = hostname,
        dbname = DB_Name,
        user = username,
        password = pws,
        port = port_id
  )
curs = conn.cursor()
#Create Tables
curs.execute(TableSchema)
conn.commit()

#Close DB
curs.close()
conn.close()
