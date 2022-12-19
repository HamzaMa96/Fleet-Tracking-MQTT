import psycopg2
import pyodbc
# SQLite DB Name
# DB_Name =  "mqtt"
# hostname = 'localhost'
# username = 'postgres'
# pws = 'password@1324'
# port_id = 5432
# SQL Server DB credentials
conn= pyodbc.connect('driver={SQL Server}; server=ARCGISDB; database=TrackingDB; UID=sa; PWD=Pa$$w0rd;')
# SQL Server Table Schema
TableSchema="""
drop table if exists engine_status ;
drop table if exists Working_Hours ;
drop table if exists Battery_Level ;
drop table if exists Device_Tampering ;
drop table if exists Engine_RPM ;
drop table if exists location ;
drop table if exists Fake_IOT_Data ;
drop table if exists IOT_Data_2 ;
drop table if exists IOT_Data3 ;

create table IOT_Data3 (
  id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
  deviceid int,
  DateTime DATETIME,
  EngineStatus int,
  WorkingHours float,
  BatteryLevel int,
  DeviceTampering int,
  EngineRPM float,
  longitude float,
  latitude float,
  shape varchar(150)
);
"""

#Connect or Create DB File
conn= pyodbc.connect('driver={SQL Server}; server=ARCGISDB; database=TrackingDB; UID=sa; PWD=Pa$$w0rd;')
curs = conn.cursor()

#Create Tables
curs.execute(TableSchema)
conn.commit()

#Close DB
curs.close()
conn.close()
