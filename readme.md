# etl-api-to-postgres

Este proyecto describe el proceso de: extracción de datos desde una API, containerización con Docker, gestión de autenticación API, manejo de paginación, serialización en formato JSON, verificación de conexión a base de datos y carga de datos en tablas.

<h2>Tecnologias Usadas</h2>

* Docker
* Postgres
* Python

## To start this project

### create a `.env` file

Create a .env file in the root of your project and get the following token from GitHub.

```
 GITHUB_TOKEN= Aqui el token
```
In the project directory, you need to:
### `docker compose up --build `

Install all the packages

![run-on-terminal](https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExcjgzamRveWFiM21qbGJ1NTJhZWowNzE3c2Jub2tiZDc3a2FtdGw1dyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/y4Pjh8raB5PkIAdBMn/giphy.gif)

<h2>Resumen</h2>

* Verificamos que la base de datos este recibiendo connections:
check_db_connection()
* Conectamos la api y extraemos los datos: batch_data_fetch()
* Normalizamos y creamos el esquema: process_event()
* Carga de datos en tablas

<h2>Features piece of code no.1</h2>


``` Python

def batch_data_fetch():

    TOKEN_GIT = os.getenv("GITHUB_TOKEN")
    page_number = 0
    base_url = "url"

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
        print(response.links)  
        page_number += 1
        base_url = next_page

        if next_page is None:
            print("No hay mas paginas")
            break   

        with open(f"/data/page_{page_number}.json", "w") as f:
            json.dump(page_data, f)

    return all_events

```

<h2>Features piece of code no.2</h2>


``` Docker

FROM python:3.11-slim

RUN apt-get update && apt-get install -y postgresql-client

WORKDIR /src

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "pipeline.py"]

```
