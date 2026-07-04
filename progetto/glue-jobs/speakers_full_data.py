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

df_ted_raw = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(ted_data_path)

df_ted = df_ted_raw.select("slug", "viewedCount").dropDuplicates(["slug"])

##### GET DATA FROM MONGODB ATLAS
secret = get_secret()

username = secret["username"]
password = secret["password"]

db_name = "unibg_tedx_2026"
db_uri = f"mongodb+srv://{username}:{password}@cluster0.hduxclv.mongodb.net/?appName=Cluster0"

df_speakers = mongo_collection_to_DF(db_uri, db_name, "speaker_with_talks")
df_youtube = mongo_collection_to_DF(db_uri, db_name, "ted_with_yt_data")

# 1. Appiattisci l'array dei talk
speakers_flat = df_speakers.withColumn("single_talk", F.explode("talks"))

# Seleziona solo i campi necessari da YouTube per evitare collisioni di nomi
youtube_min = df_youtube.select("slug", "yt_statistics")

# 2. Esegui il join sullo 'slug'
joined_df = speakers_flat.join(
    youtube_min,
    speakers_flat["single_talk.slug"] == youtube_min["slug"],
    "left"
)

joined_df = joined_df.join(
    df_ted,
    joined_df["single_talk.slug"] == df_ted["slug"],
    "left"
)

# 3. Costruisci la nuova struttura del talk includendo le statistiche di YouTube
updated_talk_struct = F.struct(
    F.col("single_talk.duration").alias("duration"),
    F.col("single_talk.publishedAt").alias("publishedAt"),
    F.col("single_talk.slug").alias("slug"),
    F.col("single_talk.title").alias("title"),
    F.col("single_talk.url").alias("url"),
    F.struct(
        F.col("viewedCount").alias("viewCount_ted"),
        F.col("yt_statistics.viewCount").alias("viewCount_yt"),
        F.col("yt_statistics.commentCount").alias("commentCount_yt"),
        F.col("yt_statistics.likeCount").alias("likeCount_yt"),
        F.col("yt_statistics.favoriteCount").alias("favoriteCount_yt"),
    ).alias("statistics")
)

# 4. Raggruppa per speaker e ricostruisci l'array 'talks'
final_df = joined_df.withColumn("new_talk", updated_talk_struct) \
    .groupBy("_id", "speaker") \
    .agg(F.collect_list("new_talk").alias("talks"))

# Mostra il risultato
final_df.printSchema()

##### WRITE TO MONGODB ATLAS
write_mongo_options = {
    "connection.uri": db_uri,
    "database": db_name,
    "collection": "speakers_full_data",
    "ssl": "true",
    "ssl.domain_match": "false"
}

from awsglue.dynamicframe import DynamicFrame
result = DynamicFrame.fromDF(final_df, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(result, connection_type="mongodb", connection_options=write_mongo_options)
