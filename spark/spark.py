from __future__ import print_function
from elasticsearch import Elasticsearch
# import pandas as pd
from datetime import datetime,timedelta,date

import sys
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.dataframe import DataFrame
import time
import hashlib
import csv
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor,LinearRegression,GBTRegressor
from pyspark.sql.types import IntegerType,DoubleType
from pyspark.sql.functions import array_contains,current_timestamp
from pyspark.sql.functions import col
import os


def predict_viewers(item,data,assembler):

  filtered_data = None

  if 'game_id' in item:
    filtered_data = data.filter(data["game_id"] == item["twitch-data"]["stream"]['game_id'])
    

  regressor = LinearRegression(featuresCol="features", labelCol="viewer_count")

# Addestramento del modello in base a tutto il dataset o solo alla saga
  model = None
  if filtered_data is None or filtered_data.isEmpty():
    model = regressor.fit(data)
  else:
    model = regressor.fit(filtered_data)
  new_data = spark.createDataFrame([(item["twitch-data"]["stream"]['follower_count'], item["twitch-data"]['total_rating'], item["twitch-data"]["stream"]['viewer_count'])], ["follower_count", "total_rating", "viewer_count"])
  new_data_assembled = assembler.transform(new_data)
  predicted_values = model.transform(new_data_assembled)

  first_prediction = predicted_values.select(col("prediction").cast("int")).first()[0]
  return first_prediction

def update_csv(file_path, id_da_eliminare,listItem):
    # Crea una lista per tenere traccia degli elementi da mantenere
    elementi_da_mantenere = []

    # Apri il file CSV in modalità lettura
    with open(file_path, 'r') as file:
        # Leggi i dati del file CSV
        reader = csv.reader(file)

        # Itera attraverso le righe del file CSV
        for row in reader:
            # Verifica se il titolo corrente corrisponde a quello da eliminare
            if row[0] not in id_da_eliminare: 
                # Aggiungi la riga alla lista degli elementi da mantenere
                elementi_da_mantenere.append(row) 

        for id in id_da_eliminare:
              tempRow = listItem[id]
              idRow = tempRow["id"]
              colReleaseDate = date.today().strftime("%d/%m/%Y")
              colUserName = tempRow["twitch-data"]["stream"]["user_name"]
              colUserId = tempRow["twitch-data"]["stream"]["user_id"]

              colLanguage = tempRow["twitch-data"]["stream"]["language"]

              colGameName = tempRow["twitch-data"]["stream"]["game_name"]
              colGameId = tempRow["twitch-data"]["stream"]["game_id"]
              colViewerCount = tempRow["twitch-data"]["stream"]["viewer_count"]
              colTotalRating = tempRow["twitch-data"]["total_rating"]
              colFollowerCount = tempRow["twitch-data"]["stream"]["follower_count"]

              genre_idsMColl = tempRow["genre_ids"]
# 2023-07-21 16:16:39,Gamers8GG_b,en,Dota 2,39240,85.23121689955448,2013-07-09,"Strategy, MOBA"

              elementi_da_mantenere.append([idRow,colReleaseDate,colUserName,colUserId,colFollowerCount,colLanguage,colGameName,colGameId,colViewerCount,colTotalRating,genre_idsMColl])  


    # Sovrascrivi il file originale con i dati filtrati
    with open(file_path, 'w', newline='') as file:
        # Scrivi i dati filtrati sul file CSV
        writer = csv.writer(file)
        writer.writerows(elementi_da_mantenere)

    # Chiudi il file
    file.close()

def elaborate(batch_df: DataFrame, batch_id: int):
  batch_df.show(truncate=False)

sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)
sc.setLogLevel("WARN")
date.today().strftime("%B,%d,%Y")
kafkaServer="kafka:39092"
topic = "twitch_topic"
# Funzione per inviare i dati a Elasticsearch
def send_to_elasticsearch(batch_df: DataFrame, batch_id: int):
    es = Elasticsearch({'host': 'elasticsearch', 'port': 9200, 'use_ssl': False}, timeout=30, max_retries=10, retry_on_timeout=True)  # Indirizzo IP o hostname di Elasticsearch
    if not es.ping():
        raise ValueError("Impossibile connettersi a Elasticsearch")

    idsList = []
    objList={}
    records = batch_df.toJSON().map(json.loads).collect()

    for record in records:
      converted_dict = json.loads(record["value"])
      converted_dict['id'] = date.today().strftime("%B,%d,%Y") + converted_dict['twitch-data']['stream']['user_id'] +","+ converted_dict['twitch-data']['stream']['game_id']
      print(converted_dict['id'])
      converted_dict['id'] = hashlib.md5(converted_dict['id'].encode('utf-8')).hexdigest()
      print(converted_dict['id'])
      idsList.append(converted_dict['id'])
      objList[converted_dict['id']] = converted_dict
    
    update_csv('./data/data.csv',idsList,objList)

    # Conversione del DataFrame in una lista di dizionari
    df = spark.read.csv("./data/data.csv", header=True, inferSchema=True)
    df = df.withColumn("follower_count", df["follower_count"].cast(IntegerType()))
    df = df.withColumn("total_rating", df["total_rating"].cast(DoubleType()))
    df = df.withColumn("viewer_count", df["viewer_count"].cast(IntegerType()))
    df = df.withColumn("Ptimestamp", current_timestamp())

    df = df.na.drop()
    selected_features = ["follower_count", "total_rating", "viewer_count"]
    assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
    data = assembler.transform(df).select("features","follower_count", "total_rating", "viewer_count","game_id","game_name")

    # Invio dei dati a Elasticsearch
    for record in records:
        converted_dict = json.loads(record["value"])
        prediction = predict_viewers(converted_dict,data,assembler)
        prediction.show()
        if prediction is not None and prediction >= 0:
          converted_dict["predicted_viewers"] = prediction
        doc_id =  int(str(time.time()).replace(".", ""))   # Utilizza l'id come id del documento in Elasticsearch
 
        update_body = {
            "doc": converted_dict,
            "doc_as_upsert": True
        }

        es.update(index="twitch", id=doc_id, body=update_body)

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