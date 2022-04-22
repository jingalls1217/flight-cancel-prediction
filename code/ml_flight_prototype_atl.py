## Must run the following python programs. This program must run after #1 below
#     1) ml_flight_prototype_ord.py
#     2) ml_flight_prototype_atl.py
#     3) ml_flight_prototype_sfo.py


import cml.data_v1 as cmldata

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *


CONNECTION_NAME = "jing-airline-vw"
conn = cmldata.get_connection(CONNECTION_NAME)

## Query Flights to get pandas data frame
SQL_QUERY = '''SELECT 
cast(concat_ws("-",cast(`year` as string), cast(`month` as string), cast(`dayofmonth` as string)) AS date) AS fl_date,
cast(deptime AS string) AS dep_time,
cast(crsdeptime AS string) AS crs_dep_time,
cast(arrtime AS string) AS arr_time,
cast(crsarrtime AS string) AS crs_arr_time,
uniquecarrier AS op_carrier,
cast(flightnum AS string) AS op_carrier_fl_num,
actualelapsedtime AS actual_elapsed_time,
crselapsedtime AS crs_elapsed_time,
airtime AS air_time,
arrdelay AS arr_delay,
depdelay AS dep_delay,
origin,
dest,
distance,
taxiin AS taxi_in,
taxiout AS taxi_out,
cancelled,
cancellationcode AS cancellation_code,
cast(diverted AS double) AS diverted,
carrierdelay AS carrier_delay,
weatherdelay AS weather_delay,
nasdelay AS nas_delay,
securitydelay AS security_delay,
lateaircraftdelay AS late_aircraft_delay
FROM airline_ontime_orc.flights
WHERE origin = "ATL" and year >= 2004'''

dataframe = conn.get_pandas_dataframe(SQL_QUERY)
dataframe.head(10)

## Write data to flight_prototype table in default DBC
#    - Must write to default DBC for Data Catalog profilers to work
spark = SparkSession\
    .builder\
    .config("spark.datasource.hive.warehouse.load.staging.dir", "/tmp")\
    .config("spark.executor.memory", "8g")\
    .config("spark.executor.cores", "2")\
    .config("spark.driver.memory", "6g")\
    .config("spark.executor.instances", "4")\
    .config("spark.rpc.message.maxSize", "2047")\
    .config("spark.yarn.access.hadoopFileSystems", os.environ["STORAGE"])\
    .appName("PythonSQL")\
    .getOrCreate()

## Create Airlines Database in default DB
spark.sql("CREATE DATABASE IF NOT EXISTS airlines")

# Load Spark dataframe from Pandas dataframe
df = spark.createDataFrame(dataframe) 
df.printSchema()
df.show()

# Create flight_prototype table in default DBC
df.write.format('orc').mode("append").saveAsTable("airlines.flight_prototype")


## Query flights data for more data into Pandas dataframe
SQL_QUERY = '''SELECT 
cast(concat_ws("-",cast(`year` as string), cast(`month` as string), cast(`dayofmonth` as string)) AS date) AS fl_date,
cast(deptime AS string) AS dep_time,
cast(crsdeptime AS string) AS crs_dep_time,
cast(arrtime AS string) AS arr_time,
cast(crsarrtime AS string) AS crs_arr_time,
uniquecarrier AS op_carrier,
cast(flightnum AS string) AS op_carrier_fl_num,
actualelapsedtime AS actual_elapsed_time,
crselapsedtime AS crs_elapsed_time,
airtime AS air_time,
arrdelay AS arr_delay,
depdelay AS dep_delay,
origin,
dest,
distance,
taxiin AS taxi_in,
taxiout AS taxi_out,
cancelled,
cancellationcode AS cancellation_code,
cast(diverted AS double) AS diverted,
carrierdelay AS carrier_delay,
weatherdelay AS weather_delay,
nasdelay AS nas_delay,
securitydelay AS security_delay,
lateaircraftdelay AS late_aircraft_delay
FROM airline_ontime_orc.flights
WHERE origin = "ATL" and (year >= 2000 and year < 2004)'''

dataframe = conn.get_pandas_dataframe(SQL_QUERY)
dataframe.head(10)

# Load Spark dataframe from Pandas dataframe
df = spark.createDataFrame(dataframe) 
df.printSchema()
df.show()

# Append more data to flight_prototype table in default DBC
df.write.format('orc').mode("append").saveAsTable("airlines.flight_prototype")


## Stop Spark Session
spark.stop()
