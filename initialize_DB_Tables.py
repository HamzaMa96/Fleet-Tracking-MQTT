import psycopg2

# SQLite DB Name
DB_Name =  "mqtt"
hostname = 'localhost'
username = 'postgres'
pws = 'password@1324'
port_id = 5432

# SQLite DB Table Schema
TableSchema="""
drop table if exists DHT22_Temperature_Data ;
drop table if exists DHT22_Humidity_Data ;

create table Engine_Status (
  id serial primary key,
  Date_n_Time timestamp,
  Engine_Status int
);

create table Working_Hours (
  id serial primary key,
  Date_n_Time timestamp,
  WorkingHours float
);

create table Battery_Level (
  id serial primary key,
  Date_n_Time timestamp,
  BatteryLevel int
);

create table Device_Tampering (
  id serial primary key,
  Date_n_Time timestamp,
  DeviceTampering int
);

create table Engine_RPM (
  id serial primary key,
  Date_n_Time timestamp,
  EngineRPM float
);

create table location (
  id serial primary key,
  Date_n_Time timestamp,
  x float,
  y float
);
"""

#Connect or Create DB File
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
