FROM python:3.11-slim 

RUN apt-get update && apt-get install -y postgresql-client 

WORKDIR /src

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "pipeline.py"]


