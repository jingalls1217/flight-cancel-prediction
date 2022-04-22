## Create flight_simulation table from flight_prototype table
import cml.data_v1 as cmldata

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession\
    .builder\
    .config("spark.datasource.hive.warehouse.load.staging.dir", "/tmp")\
    .config("spark.yarn.access.hadoopFileSystems", os.environ["STORAGE"])\
    .appName("PythonSQL")\
    .getOrCreate()

# Sample usage to run query through spark
SQL = '''CREATE TABLE airlines.flight_simulation AS SELECT
concat_ws("*",CAST(fl_date AS STRING),op_carrier,op_carrier_fl_num,origin,dest,CAST(crs_dep_time AS STRING)) AS FL_ID,
CAST(fl_date AS STRING) AS FL_DATE,
op_carrier AS OP_CARRIER,
op_carrier_fl_num AS OP_CARRIER_FL_NUM,
origin AS ORIGIN,
dest AS DEST,
CAST(crs_dep_time AS STRING) AS CRS_DEP_TIME,
CAST(crs_arr_time AS STRING) AS CRS_ARR_TIME,
CAST(cancelled AS STRING) AS CANCELLED,
CAST(crs_elapsed_time AS STRING) AS CRS_ELAPSED_TIME,
CAST(distance AS STRING) AS DISTANCE,
CAST(left(lpad(crs_dep_time,4,"0"),2) AS STRING) AS HOUR,
CAST(weekofyear(fl_date) AS STRING) AS WEEK,
CASE WHEN cancelled = 1 THEN "YES" ELSE "NO" END AS cancel
FROM airlines.flight_prototype'''

spark.sql(SQL)

spark.stop()
