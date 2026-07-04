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
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

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

# Carichiamo la collezione dei transcript
df_tscripts = mongo_collection_to_DF(db_uri, db_name, "transcripts")

# 1. APPIATTIMENTO DEI SOTTOTITOLI (CUES)
# Esplodiamo prima i paragrafi contenuti in data.translation.paragraphs
df_flat = df_tscripts.withColumn("paragraph", F.explode("data.translation.paragraphs"))

# Esplodiamo i cues all'interno di ogni paragrafo per arrivare al singolo testo/time
df_flat = df_flat.withColumn("cue", F.explode("paragraph.cues"))

# Creiamo l'oggetto finale richiesto: { "text": ..., "timestamp": ... }
df_flat = df_flat.withColumn(
    "single_cue", 
    F.struct(
        F.col("cue.text").alias("text"),
        F.col("cue.time").alias("timestamp")
    )
)

# Raggruppiamo nuovamente per ricostruire l'array di oggetti (testo e timestamp) per documento
# Estraiamo anche la lingua (englishName o id a tua scelta, qui uso englishName)
df_recomposed = df_flat.groupBy("_id", "slug", "data.translation.language.englishName") \
    .agg(F.collect_list("single_cue").alias("cues_list"))

# 2. COSTRUZIONE DELLA STRUTTURA FINALE RICHIESTA
# Struttura interna di translation: { "language": "...", "cues": [...] }
translation_struct = F.struct(
    F.col("englishName").alias("language"),
    F.col("cues_list").alias("cues")
)

# Struttura finale: inseriamo tutto dentro l'oggetto "data"
final_df = df_recomposed.withColumn(
    "data", 
    F.struct(translation_struct.alias("translation"))
).select("_id", "slug", "data")

##### WRITE TO MONGODB ATLAS
write_mongo_options = {
    "connection.uri": db_uri,
    "database": db_name,
    "collection": "transcripts_flat",  # Cambiata per evitare di sovrascrivere l'originale se non desiderato
    "ssl": "true",
    "ssl.domain_match": "false"
}

result = DynamicFrame.fromDF(final_df, glueContext, "nested")
glueContext.write_dynamic_frame.from_options(result, connection_type="mongodb", connection_options=write_mongo_options)

job.commit()