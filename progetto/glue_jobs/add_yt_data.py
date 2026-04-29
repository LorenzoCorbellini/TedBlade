import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
from botocore.exceptions import ClientError

###### HELPER FUNCTIONS

def get_secret():

    secret_name = "MongoBD"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

# Return a Spark DataFrame from a MongoDB collection
def mongo_collection_to_DF(db_uri, db_name, collection_name):
    return glueContext.create_dynamic_frame_from_options(
        connection_type = "mongodb",
        connection_options = {
            "connection.uri": db_uri,
            "database": db_name,
            "collection": collection_name
        }
    ).toDF()

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### INIT GLUE CONTEXT AND SPARK CONTEXT
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

##### GET DATA FROM MONGODB ATLAS

secret = get_secret()

username = secret["username"]
password = secret["password"]

db_name = "unibg_tedx_2026"
db_uri = f"mongodb+srv://{username}:{password}@cluster0.hduxclv.mongodb.net/?appName=Cluster0"

related_videos = mongo_collection_to_DF(db_uri, db_name, "tedx_data_related")
matches = mongo_collection_to_DF(db_uri, db_name, "matches")
yt_data = mongo_collection_to_DF(db_uri, db_name, "video_stats")

##### RENAME AND SELECT COLUMNS

matches = matches.withColumnRenamed("_id", "id_ref")

yt_data = yt_data.withColumnRenamed("id", "yt_id") \
            .withColumnRenamed("snippet", "yt_snippet") \
            .withColumnRenamed("statistics", "yt_statistics") \
            .select("yt_id", "yt_snippet", "yt_statistics")

##### JOIN(S)

# Data from 'related videos' -> we add youtube ids
rv_ytids = related_videos.join(matches, related_videos.slug == matches.slug, "left") \
    .drop("id_ref") \
    .drop("score") \
    .drop("slug")

# Add data from youtube (views, likes, etc.)
df1 = rv_ytids.join(yt_data, rv_ytids.yt_id == yt_data.yt_id, "left") \
        .drop(yt_data["yt_id"])

df1.show(5, truncate=False)

##### WRITE TO MONGODB ATLAS

write_mongo_options = {
        "connection.uri": db_uri,
        "database": db_name,
        "collection": "ted_with_yt_data",
        "ssl": "true",
        "ssl.domain_match": "false"
    }

from awsglue.dynamicframe import DynamicFrame
result = DynamicFrame.fromDF(df1, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(result, connection_type="mongodb", connection_options=write_mongo_options)
