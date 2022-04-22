# ###########################################################################
#
#  Model Operations - Drift Simulation
#  
# The script will take 1000 samples from the data set and simulate 1000
# predictions. The live model will be called each time in the loop and while the
# `churn_error` function adds an increasing amount of error to the data to make
# the model less accurate. The actual value, the response value, and the uuid are
# added to an array.
#
# Then there is "ground truth" loop that iterates though the array and updates the
# recorded metric to add the actual label value using the uuid. At the same time, the
# model accruacy is evaluated every 100 samples and added as an aggregate metric.
# Overtime this accuracy metric falls due the error introduced into the data.
#
# ###########################################################################

import cdsw, time, os, random, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import classification_report
from cmlbootstrap import CMLBootstrap
import copy
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sqlite3

hive_database = "airlines"
hive_table = "flight_simulation"
hive_table_fq = hive_database + "." + hive_table

## Read flight_simulation data into a Spark DataFrame
spark = SparkSession\
    .builder\
    .config("spark.datasource.hive.warehouse.load.staging.dir", "/tmp")\
    .config("spark.executor.memory", "8g")\
    .config("spark.executor.cores", "2")\
    .config("spark.driver.memory", "6g")\
    .config("spark.executor.instances", "4")\
    .config("spark.yarn.access.hadoopFileSystems", os.environ["STORAGE"])\
    .appName("PythonSQL")\
    .getOrCreate()

airline_data_raw = spark.sql("SELECT * FROM " + hive_table_fq)

airline_data_raw.show(n=10)

df = airline_data_raw.toPandas()

## Get the various Model CRN details
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]
API_KEY = os.getenv("CDSW_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

## Get newest deployed cancel model details using cmlbootstrapAPI
models = cml.get_models({})

cancel_model_details = [
    model
    for model in models
    if model["name"] == "Flight Delay Prediction Model Endpoint"
    and model["creator"]["username"] == USERNAME
    and model["project"]["slug"] == PROJECT_NAME
][0]
latest_model = cml.get_model(
    {
        "id": cancel_model_details["id"],
        "latestModelDeployment": True,
        "latestModelBuild": True,
    }
)

Model_CRN = latest_model["crn"]
Deployment_CRN = latest_model["latestModelDeployment"]["crn"]
model_endpoint = (
    HOST.split("//")[0] + "//modelservice." + HOST.split("//")[1] + "/model"
)

# print(models)

#
## Write Model details to Hive Table
hive_database = "airlines"
hive_table = "models"
hive_table_fq = hive_database + "." + hive_table

# Issue starts with conversion to Spark DataFrame; seems to only happen in Map data types
structureSchema = StructType([
      StructField('id', StringType(), True),
      StructField('projectId', IntegerType(), True),
      StructField('project', StructType([
         StructField('id', IntegerType(), True),
         StructField('name', StringType(), True),
         StructField('slug', StringType(), True),
         StructField('crn', StringType(), True),
         StructField('default_project_engine_type', StringType(), True)
      ])),
      StructField('projectOwner', StructType([
         StructField('id', IntegerType(), True),
         StructField('username', StringType(), True),
         StructField('type', StringType(), True)
      ])),
      StructField('crn', StringType(), True),
      StructField('creatorId', IntegerType(), True),
      StructField('creator', StructType([
         StructField('id', IntegerType(), True),
         StructField('username', StringType(), True),
         StructField('type', StringType(), True)
      ])),
      StructField('name', StringType(), True),
      StructField('description', StringType(), True),
      StructField('visibility', StringType(), True),
      StructField('accessKey', StringType(), True),
      StructField('authEnabled', BooleanType(), True),
      StructField('defaultResources', StructType([
         StructField('cpuMillicores', IntegerType(), True),
         StructField('memoryMb', IntegerType(), True)
      ])),
      StructField('defaultReplicationPolicy', StructType([
         StructField('type', StringType(), True),
         StructField('numReplicas', IntegerType(), True)
      ])),
      StructField('htmlUrl', StringType(), True),
      StructField('createdAt', StringType(), True),
      StructField('updatedAt', StringType(), True),
      StructField('namespace', StringType(), True)
    ])

models_sdf = spark.createDataFrame(data=models,schema=structureSchema)

models_sdf.printSchema()
models_sdf.show(20, False)

## Create Models table in Hive
models_sdf.createOrReplaceTempView(hive_table + "_temp")

spark.sql("drop table if exists " + hive_table_fq)
spark.sql("create table " + hive_table_fq  + " as select * from " + hive_table + "_temp")

## Function to skew actual Cancel results 
# This will randomly return True for input and increases the likelihood of returning
# true based on `percent`
def cancel_error(item, percent):
    if random.random() < percent:
        return True
    else:
        return True if item == "Yes" else False

# Get 10000 samples
df_sample = df.sample(10000)

df_sample.groupby("cancel")["cancel"].count()

df_sample_clean = (
    df_sample.replace({"cancel": {"YES": "Yes", "NO": "No"}})
    .replace({"cancelled": {"1": "Yes", "0": "No"}})
    .replace(r"^\s$", np.nan, regex=True)
    .dropna()
)

# check cleaned sample
df_sample_clean.head(10)

# Create an array of model responses.
response_labels_sample = []

# Run Similation to make 10000 calls to the model with increasing error
percent_counter = 0
percent_max = len(df_sample_clean)

for record in json.loads(df_sample_clean.to_json(orient="records")):
    print("Added {} records".format(percent_counter)) if (
        percent_counter % 50 == 0
    ) else None
    percent_counter += 1
    no_cancel_record = copy.deepcopy(record)
    no_cancel_record.pop("FL_ID")
    no_cancel_record.pop("cancel")

    # ###########
    # Transform "no_cancel_record" into format: {"feature":"US,DCA,BOS,1,16"}
    feature_list = [no_cancel_record["OP_CARRIER"],no_cancel_record["ORIGIN"],no_cancel_record["DEST"],no_cancel_record["WEEK"],no_cancel_record["HOUR"]]
    feature = json.loads('{"feature":"' + ",".join(feature_list) + '"}')
    # ###########

    response = cdsw.call_model(latest_model["accessKey"], feature)

    # for airline cancel there are 2 metrics to track - prediction (int) & proba (string)
    # need to transform "proba" into a DOUBLE data type <OR> should it be left as string
    # "prediction" value will be 0: not expected to cancel or 1: expected to cancel
    # would like to track all "prediction" results with "proba" >= 50%

    response_labels_sample.append(
        {
            "uuid": response["response"]["uuid"],
            "final_label": cancel_error(record["cancel"], percent_counter / percent_max),
            "response_label": float(response["response"]["prediction"]["proba"]) >= 0.5,
            "timestamp_ms": int(round(time.time() * 1000)),
        }
    )

# show sample of results
with np.printoptions(threshold=np.inf):
    print(response_labels_sample)

## Track Metrics from Simulation Data
# The "ground truth" loop adds the updated actual label value and an accuracy measure
# every 100 calls to the model.
for index, vals in enumerate(response_labels_sample):
    print("Update {} records".format(index)) if (index % 50 == 0) else None
    cdsw.track_delayed_metrics({"final_label": vals["final_label"]}, vals["uuid"])
    if index % 100 == 0:
        start_timestamp_ms = vals["timestamp_ms"]
        final_labels = []
        response_labels = []
    final_labels.append(vals["final_label"])
    response_labels.append(vals["response_label"])
    if index % 100 == 99:
        print("Adding accuracy metric")
        end_timestamp_ms = vals["timestamp_ms"]
        accuracy = classification_report(
            final_labels, response_labels, output_dict=True
        )["accuracy"]
        cdsw.track_aggregate_metrics(
            {"accuracy": accuracy},
            start_timestamp_ms,
            end_timestamp_ms,
            model_deployment_crn=Deployment_CRN,
        )


# Read in the model metrics dict
model_metrics = cdsw.read_metrics(
    model_crn=Model_CRN, model_deployment_crn=Deployment_CRN
)

# This is a handy way to unravel the dict into a big pandas dataframe
metrics_df = pd.io.json.json_normalize(model_metrics["metrics"])
metrics_df.tail().T

# Write the data to SQL lite for visualization
if not (os.path.exists("model_metrics.db")):
    conn = sqlite3.connect("model_metrics.db")
    metrics_df.to_sql(name="model_metrics", con=conn)

# Do some conversions & calculations on the raw metrics
metrics_df["startTimeStampMs"] = pd.to_datetime(
    metrics_df["startTimeStampMs"], unit="ms"
)
metrics_df["endTimeStampMs"] = pd.to_datetime(metrics_df["endTimeStampMs"], unit="ms")
metrics_df["processing_time"] = (
    metrics_df["endTimeStampMs"] - metrics_df["startTimeStampMs"]
).dt.microseconds * 1000

# Display Model Metrics
metrics_df.head(10)


##  Write Model Metrics data to Hive for CDV Dashboard
# Hive Table to create
hive_database = "airlines"
hive_table = "model_metrics"
hive_table_fq = hive_database + "." + hive_table

# Schema to use for Model Metrics Data should not contain '.' chars
metrics_df.columns = metrics_df.columns.str.replace('.', '_')

# Cleanup Data
metrics_df['metrics_accuracy'] = metrics_df['metrics_accuracy'].fillna(-1.0)

metrics_df['metrics_input_data_feature'] = metrics_df['metrics_input_data_feature'].astype(str)
metrics_df['metrics_proba'] = metrics_df['metrics_proba'].astype(float)
metrics_df['metrics_final_label'] = metrics_df['metrics_final_label'].astype(bool)

metrics_df.dtypes

# Write Model Metrics Data to Hive table
metrics_sdf = spark.createDataFrame(metrics_df)

metrics_sdf.createOrReplaceTempView(hive_table + "_temp")

spark.sql("drop table if exists " + hive_table_fq)
spark.sql("create table " + hive_table_fq  + " as select * from " + hive_table + "_temp")

spark.stop()
