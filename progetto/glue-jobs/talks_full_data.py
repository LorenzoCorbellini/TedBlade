import sys
import json
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

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

##### GET DATA FROM BUCKET
ted_data_path = "s3://cloud-mobile-data-1/related_videos.csv"

df_rel_raw = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(ted_data_path)

df_related = df_rel_raw.select("slug", "viewedCount").dropDuplicates(["slug"]) # Contiene i related_videos e le views su TED (a noi interessano quelle)

##### GET DATA FROM MONGODB ATLAS
secret = get_secret()

username = secret["username"]
password = secret["password"]

db_name = "unibg_tedx_2026"
db_uri = f"mongodb+srv://{username}:{password}@cluster0.hduxclv.mongodb.net/?appName=Cluster0"

df_ted = mongo_collection_to_DF(db_uri, db_name, "ted_with_yt_data") # Collezione che contiene i dati di TED
df_wnext = mongo_collection_to_DF(db_uri, db_name, "watch_next") # Collezione che contiene il watch_next di ogni video
df_stats = mongo_collection_to_DF(db_uri, db_name, "yt_stats") # Collezione che contiene stats di youtube
df_matches = mongo_collection_to_DF(db_uri, db_name, "matches") # Collezione che associa un video TED al suo video YT (se esiste)

# Selezioniamo i dati che ci interessano 
# e rimuoviamo quelli che non ci interessano

df_stats = df_stats.select(
    F.col("id").alias("yt_id"),
    F.col("statistics").alias("yt_stats")
)

df_matches = df_matches.select(
    "slug",
    "yt_id"
)

# Rimuoviamo da df_ted
df_ted = df_ted.drop(
    "related",
    "yt_snippet",
    "yt_title"
)

##### DATA MANIPULATION

# Aggiungiamo watch_next
df_wnext = df_wnext.select("slug", F.col("watch_next.slug").alias("watch_next"))

# Aggiungiamo watch_next
df_ted = df_ted.join(
    df_wnext,
    "slug",
    "left"
)

# Aggiungiamo viewCount da TED (dal file con i related videos)
df_ted = df_ted.join(
    df_related,
    "slug",
    "left"
)

# Mettiamo le stats (views, likes, etc) in uno struct

df_ted = df_ted.withColumn(
    "statistics",
    F.struct(
        # Estraiamo i campi dallo struct aggiornato appena importato da df_stats
        F.col("yt_statistics.viewCount").alias("viewCount_yt"),
        F.col("yt_statistics.commentCount").alias("commentCount_yt"),
        F.col("yt_statistics.likeCount").alias("likeCount_yt"),
        F.col("yt_statistics.favoriteCount").alias("favoriteCount_yt"),
        F.col("viewedCount").alias("viewCount_ted"),
    )
)

# Aggiungiamo yt_id
# df_ted = df_ted.join(
#     df_matches,
#     df_ted["slug"] == df_matches["slug"],
#     "left"
# )

# Aggiungiamo stats di yt
# df_ted = df_ted.join(
#     df_stats,
#     df_ted["yt_id"] == df_stats["yt_id"],
#     "left"
# )

# Rimuoviamo i campi che non usiamo più
df_ted = df_ted.drop("yt_statistics", "viewedCount")

##### WRITE TO MONGODB ATLAS
write_mongo_options = {
    "connection.uri": db_uri,
    "database": db_name,
    "collection": "talks_full_data",
    "ssl": "true",
    "ssl.domain_match": "false"
}

from awsglue.dynamicframe import DynamicFrame
result = DynamicFrame.fromDF(df_ted, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(result, connection_type="mongodb", connection_options=write_mongo_options)
