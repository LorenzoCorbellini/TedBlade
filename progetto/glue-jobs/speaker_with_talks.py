'''
Consegna 2 b)

Questo job produce una collezione di documenti: ogni documento
rappresenta uno speaker ed elenca tutti i talks da lui presentati 
'''
import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import boto3
from botocore.exceptions import ClientError

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

# Speakers

df_speakers = mongo_collection_to_DF(db_uri, db_name, "speakers")
df_speakers = df_speakers.withColumnRenamed("speakers", "speaker")

# Talks

df_talks = mongo_collection_to_DF(db_uri, db_name, "tedx_data")

##### MANIPULATE DATA

# lo speaker di df_speakers deve essere contenuto nella stringa 'speakers' di df_talks 
# Nota: usiamo l'operatore .contains() per gestire i casi con più nomi
df_1 = df_speakers.join(df_talks, F.col("speakers").contains(F.col("speaker")))

# creiamo dei gruppi in base agli speaker
# per ogni speaker memorizziamo i talks in cui ha presentato
df_speaker_talks = df_1.groupBy("speaker").agg(
    F.collect_list(
        F.struct(
            df_talks["slug"],
            F.col("title"),
            F.col("url"),
            F.col("duration"),
            F.col("publishedAt")
        )
    ).alias("talks")
)

##### WRITE TO MONGODB ATLAS
write_mongo_options = {
    "connection.uri": db_uri,
    "database": db_name,
    "collection": "speaker_with_talks",
    "ssl": "true",
    "ssl.domain_match": "false"
}

from awsglue.dynamicframe import DynamicFrame
result = DynamicFrame.fromDF(df_speaker_talks, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(result, connection_type="mongodb", connection_options=write_mongo_options)
