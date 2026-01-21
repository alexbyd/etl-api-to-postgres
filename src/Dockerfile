FROM python:3.11-slim 

RUN apt-get update && apt-get install -y postgresql-client 

WORKDIR /src

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "pipeline.py"]



# FROM debian:bullseye


# RUN apt-get update && apt-get install -y \
#     wget \
#     unzip \
#     && rm -rf /var/lib/apt/lists/*



# RUN wget  https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-aarch64.zip\
#     && unzip duckdb_cli-linux-aarch64.zip -d /usr/local/bin \
#     && chmod +x /usr/local/bin/duckdb \
#     && rm duckdb-linux-aarch64.zip

# CMD [ "duckdb", "--version"]

#FROM python:3.8-slim

# Install PostgreSQL command-line tools
#RUN apt-get update && apt-get install -y postgresql-client-15

# Copy the ELT script 
#COPY elt_script.py .

# Set the default command to run the ELT script

# FROM debian:bullseye


# RUN apt-get update && apt-get install -y \
#     wget \
#     unzip \
#     && rm -rf /var/lib/apt/lists/*



# RUN wget  https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-aarch64.zip\
#     && unzip duckdb_cli-linux-aarch64.zip -d /usr/local/bin \
#     && chmod +x /usr/local/bin/duckdb \
#     && rm duckdb-linux-aarch64.zip

# CMD [ "duckdb", "--version"]

#FROM python:3.8-slim

# Install PostgreSQL command-line tools
#RUN apt-get update && apt-get install -y postgresql-client-15

# Copy the ELT script 
#COPY elt_script.py .

# Set the default command to run the ELT script
#CMD ["python", "elt_script.py"]

