from __future__ import print_function
from elasticsearch import Elasticsearch
# import pandas as pd
from datetime import datetime,timedelta
import sys
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.dataframe import DataFrame
import time

def elaborate(batch_df: DataFrame, batch_id: int):
  batch_df.show(truncate=False)

sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)
sc.setLogLevel("WARN")

kafkaServer="kafka:39092"
topic = "twitch_topic"

# Funzione per inviare i dati a Elasticsearch
def send_to_elasticsearch(batch_df: DataFrame, batch_id: int):
    es = Elasticsearch({'host': 'elasticsearch', 'port': 9200, 'use_ssl': False}, timeout=30, max_retries=10, retry_on_timeout=True)  # Indirizzo IP o hostname di Elasticsearch
    if not es.ping():
        raise ValueError("Impossibile connettersi a Elasticsearch")

    # Conversione del DataFrame in una lista di dizionari
    records = batch_df.toJSON().map(json.loads).collect()

    # Invio dei dati a Elasticsearch
    for record in records:
        converted_dict = json.loads(record["value"])

        doc_id =  int(str(time.time()).replace(".", ""))   # Utilizza l'id come id del documento in Elasticsearch
 
        update_body = {
            "doc": converted_dict,
            "doc_as_upsert": True
        }

        es.update(index="twitch", id=doc_id, body=update_body)
        # es.index(index="movies", id=doc_id, body=converted_dict)

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load()
# Streaming Query
df.selectExpr("CAST(timestamp AS STRING)","CAST(value AS STRING)") \
  .writeStream \
  .foreachBatch(send_to_elasticsearch) \
  .start() \
  .awaitTermination()


"""
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load()
  
df.selectExpr("CAST(timestamp AS STRING)","CAST(value AS STRING)") \
  .writeStream \
  .format("console") \
  .start() \
  .awaitTermination()
  #.foreachBatch(elaborate) \ al posto di format console
  
df.selectExpr("CAST(timestamp AS STRING)","CAST(value AS STRING)") \
  .writeStream \
  .format("console") \
  .start() \
  .awaitTermination()
"""  