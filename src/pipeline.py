#Grabbing data from an API 
#Cargas las paginas a la base de Datos

import psycopg2
import requests
import json
import subprocess
import time
from dotenv import load_dotenv
import os
import pandas as pd

max_retries = 2
host = "destination_postgres"
port='5432'

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

        with open(f"/data/page_{page_number}.json", "w") as f:
            json.dump(page_data, f)

    return all_events

# normalizacion y esquema dinamico
event = batch_data_fetch()

#extraer cada caracteristica de los Datos
print(event[0].keys())
# dict_keys(['id', 'type', 'actor', 'repo', 'payload', 'public', 'created_at', 'org'])

def process_event(event):
    result = {}

    result['id'] = event['id']
    result['type'] = event['type']
    result['public'] = event['public']
    result['created_at'] = event['created_at']
    result['actor__id'] = event['actor']['id']
    result['actor__login'] = event['actor']['login']

    return result

processed_events = []

for events in event:
    processed_event = process_event(events)
    processed_events.append(processed_event)

print(processed_events[:1])

#Agregar menejo de errores por si no coneccion a internet o a base de Datos o a docker

conn = psycopg2.connect(
        user = 'postgres',
        password = 'secret',
        host = 'destination_postgres',
        port = 5432,
        database = 'github_events'
    )

db = conn.cursor()

print(db)
db.execute("""
CREATE TABLE IF NOT EXISTS github_events(
             id TEXT PRIMARY KEY,
             type TEXT,
             public BOOLEAN,
             created_at TEXT,
             actor__id BIGINT,
             actor__login TEXT
             );
             """)

flattened_data = [
        (
            record["id"],
            record["type"],
            record["public"],
            record["created_at"],
            record["actor__id"],
            record["actor__login"]
         )
         for record in processed_events
]

db.executemany("""
INSERT INTO github_events (id, type, public, created_at, actor__id, actor__login)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""", flattened_data)

print("Data successfully loaded into PostgresQL!")


# Query and Print Data
# df = db.execute("SELECT * FROM github_events limit 2")
df = pd.read_sql_query("SELECT * FROM github_events LIMIT 2", conn)
db.close()

print("\nGitHub Events Data:")
print(df)
