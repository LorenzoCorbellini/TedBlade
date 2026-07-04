'''
Consegna 2 a)

Questo job assegna ad ogni video il video successivo watch_next

Il job determina il watch_next selezionando il video con più
tag in comune con il video di interesse
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

df = mongo_collection_to_DF(db_uri, db_name, "tedx_data")

### AI GENERATED CODE

# 1. Preparazione: seleziona le colonne necessarie ed esplodi i tag
df_tags = df.select("slug", F.explode("tags").alias("tag"))

# 2. Self-join sui tag per trovare video correlati
# Usiamo alias 'a' e 'b' per distinguere il video originale da quello suggerito
joined_df = df_tags.alias("a").join(
    df_tags.alias("b"),
    F.col("a.tag") == F.col("b.tag")
).where(F.col("a.slug") != F.col("b.slug"))

# 3. Conta i tag in comune per ogni coppia
shared_counts = joined_df.groupBy("a.slug", "b.slug").count()

# 4. Definisci una Window per trovare il video con il massimo dei tag in comune
# Il secondo criterio di ordinamento (b.slug) serve come tie-breaker
window_spec = Window.partitionBy("a.slug").orderBy(F.desc("count"), F.col("b.slug"))

# 5. Seleziona il miglior match (watch_next)
watch_next_df = shared_counts.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .select(F.col("a.slug"), F.col("b.slug").alias("watch_next"))

# 6. Unisci lo slug del suggerimento al DataFrame originale
interim_df = df.join(watch_next_df, on="slug", how="left")

# 7. Prepariamo il DataFrame dei "suggerimenti" impacchettati
df_enriched = df.select(
    F.col("slug").alias("watch_next_slug"), 
    F.struct([F.col(c) for c in df.columns]).alias("watch_next_details")
)

# 8. Join finale: colleghiamo lo slug calcolato ai dettagli del video
final_df = interim_df.join(
    df_enriched, 
    interim_df.watch_next == df_enriched.watch_next_slug, 
    "left"
)

# 9. Pulizia finale: trasformiamo 'watch_next' da stringa a oggetto e rimuoviamo i doppioni
final_df = final_df.withColumn("watch_next", F.col("watch_next_details")) \
                   .drop("watch_next_details", "watch_next_slug")

### WRITE TO MONGODB ATLAS
write_mongo_options = {
    "connection.uri": db_uri,
    "database": db_name,
    "collection": "watch_next",
    "ssl": "true",
    "ssl.domain_match": "false"
}

from awsglue.dynamicframe import DynamicFrame
result = DynamicFrame.fromDF(final_df, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(result, connection_type="mongodb", connection_options=write_mongo_options)
