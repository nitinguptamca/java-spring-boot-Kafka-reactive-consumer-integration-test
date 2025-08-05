High-Level Overview
Read the CSV file from Azure Data Lake

Parse the file using Spark (by comma)

Query MongoDB based on message_id

Update header fields based on MongoDB results

Write error records to an error file

Send messages to reprocessing event Kafka

---------
 Step 0: Set up Spark Session with MongoDB connector
python
Copy
Edit
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataLakeProcessing") \
    .config("spark.mongodb.input.uri", "mongodb://<username>:<password>@<host>/<db>") \
    .config("spark.mongodb.output.uri", "mongodb://<username>:<password>@<host>/<db>") \
    .getOrCreate()
🔹 Step 1: Read the CSV file from Azure Data Lake
python
Copy
Edit
df = spark.read.option("header", True).option("inferSchema", True).csv("abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/file.csv")
🔹 Step 2: Check MongoDB for each message_id
You’ll use foreachPartition or UDF (User Defined Function) for row-wise logic. This approach requires fetching data from MongoDB using pymongo inside a UDF.

⚙️ Define UDF to check MongoDB
python
Copy
Edit
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import pymongo

def check_peh_reprocessing(message_id):
    client = pymongo.MongoClient("mongodb://<username>:<password>@<host>")
    db = client["<db>"]
    found = db.payload.find_one({"message_id": message_id})
    return False if found else True

def check_apid_reprocessing(message_id):
    client = pymongo.MongoClient("mongodb://<username>:<password>@<host>")
    db = client["<db>"]
    found = db.acknowledge.find_one({"message_id": message_id})
    return False if found else True

# Register UDFs
peh_udf = udf(check_peh_reprocessing, BooleanType())
apid_udf = udf(check_apid_reprocessing, BooleanType())

# Apply UDFs
df = df.withColumn("PehReprocessingRequired", peh_udf(df["message_id"]))
df = df.withColumn("APIDReprocessingRequired", apid_udf(df["message_id"]))
⚠️ This approach can be slow for 1GB data due to UDF + MongoDB lookups. If possible, load Mongo collections as DataFrames and do join instead (faster). Let me know if you want that version.

🔹 Step 3: Write error records to error file
Assume error = any null message_id or processing failure:

python
Copy
Edit
from pyspark.sql.functions import col

error_df = df.filter(col("message_id").isNull())
error_df.write.mode("overwrite").csv("abfss://<container>@<storage_account>.dfs.core.windows.net/errors/error_file.csv")
🔹 Step 4: Send to Reprocessing Event
Let’s assume you're using Azure Event Hubs or Kafka. Here's a simulated example (Kafka used here for generality):

python
Copy
Edit
from pyspark.sql.functions import to_json, struct

# Create message and header structure
df = df.withColumn("message", to_json(struct([col for col in df.columns])))
df = df.withColumn("headers", to_json(struct(
    col("message_id"),
    col("message_seq_id"),
    col("schema_type"),
    col("PehReprocessingRequired"),
    col("APIDReprocessingRequired")
)))

# Write to Kafka topic (or Event Hub if configured)
df.selectExpr("CAST(message AS STRING) AS value", "CAST(headers AS STRING) AS headers") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "<kafka_broker>:9092") \
  .option("topic", "reprocessing-topic") \
  .save()
