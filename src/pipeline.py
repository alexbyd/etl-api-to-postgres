#Grabbing data from an API 
#Cargas las paginas a la base de Datos

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import psycopg2
import requests
import json
import subprocess
import time
from dotenv import load_dotenv
import os
import pandas as pd


spark = SparkSession.builder \
    .appName("GitHubAnalyticsStrict") \
    .config("spark.jars", "./postgresql-42.7.2.jar") \
    .getOrCreate()

max_retries = 2
host = "destination_postgres"
port='5432'

# host = "localhost"


def check_db_connection(max_retries,host,user=None, password=None):
    retries = 0
    while retries < max_retries:
        try:
            command = ["pg_isready", "-h", host] 
            if user:
                command.extend(["-u", user])
            if password:
                command.extend(["-p" + password])
            result = subprocess.run(
                command, check=True, capture_output=True, text=True
                    )
            if "accepting connections" in result.stdout:
                print("coneccion exitosa")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error al connectar psql: {e}")
            retries += 1
            time.sleep(5)
    print("maximo intentos alcanzados")
    return False

check_db_connection(max_retries,host)

# Conectas la api y extraes las paginas

load_dotenv()
def batch_data_fetch():
    
    TOKEN_GIT = os.getenv("GITHUB_TOKEN")
    page_number = 0
    base_url = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp/events"
    
    headers = {"Authorization": f"Bearer {TOKEN_GIT}"}
    all_events = []

    while True:
        
        response = requests.get(base_url, headers=headers)
        
        if response.status_code != 200:
            print(f"error: Status code {response.status_code}")
            break
        
        page_data = response.json()
        print(f"Pagina {page_number}: {len(page_data)} eventos")
        all_events.extend(page_data)     

        next_page = response.links.get("next",{}).get("url")
        print(response.links) # Esta linea esta imprimiendo los links de cada pagina 
        page_number += 1
        base_url = next_page

        if next_page is None:
            print("No hay mas paginas")
            break   

        with open(f"../data/page_{page_number}.json", "w") as f:
            json.dump(page_data, f)

    return all_events

# normalizacion y esquema dinamico
event = batch_data_fetch()

#extraer cada caracteristica de los Datos
print(event[0].keys())
# dict_keys(['id', 'type', 'actor', 'repo', 'payload', 'public', 'created_at', 'org'])

#========================================================================

def process_event(event):
    result = {}
    result['id'] = event['id']
    result['type'] = event['type']
    result['public'] = event['public']
    result['created_at'] = event['created_at']
    result['actor__id'] = event['actor']['id']
    result['actor__login'] = event['actor']['login']
    return result

processed_events = [process_event(e) for e in event]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True),
    StructField("actor__id", LongType(), True),
    StructField("actor__login", StringType(), True)
])

processed_events = [process_event(e) for e in event]
df = spark.createDataFrame(processed_events, schema=schema)

df_final = df.withColumn("created_at", F.to_timestamp("created_at"))

print("Esquema definido en Spark:")
df_final.printSchema()
df_final.show(5)

# Escritura a PostgreSQL
try:
    df_final.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://destination_postgres:5432/github_events")\
        .option("dbtable", "github_events_spark") \
        .option("user", "postgres") \
        .option("password", "secret") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
except Exception as e:
    print(f"Error al cargar en Postgres: {e}")

spark.stop()


