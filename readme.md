
---

# GitHub Events ETL Pipeline

Este proyecto es un pipeline de ingeniería de datos robusto que extrae, transforma y carga (ETL) eventos en tiempo real de la API de GitHub. Utiliza **Apache Spark** para el procesamiento distribuido y **PostgreSQL** como almacenamiento persistente, todo orquestado mediante **Docker**.

##  Arquitectura del Sistema

El pipeline está diseñado siguiendo las mejores prácticas de portabilidad y escalabilidad:

1. **Extracción (API REST):** Consumo de la API de GitHub con manejo de paginación automática mediante el recorrido de encabezados `Link`.
2. **Procesamiento (Apache Spark):** * Limpieza y tipado de datos crudos.
* **Flattening:** Aplanado de estructuras JSON anidadas (campos de `actor`).
* **Schema Enforcement:** Definición estricta de tipos (`StructType`) para asegurar la integridad.


3. **Carga (PostgreSQL):** Persistencia de los datos transformados mediante JDBC.

##  Stack Tecnológico

* **Lenguaje:** Python 3.11
* **Procesamiento:** PySpark (Apache Spark 3.x)
* **Base de Datos:** PostgreSQL 15
* **Infraestructura:** Docker & Docker Compose
* **Entorno:** Virtualenv / JRE (Java Runtime Environment) en contenedor.

##  Modelo de Datos (Esquema Spark)

El pipeline transforma los eventos complejos en una tabla relacional limpia:

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `id` | `String` | ID único del evento |
| `type` | `String` | Tipo de evento (WatchEvent, ForkEvent, etc.) |
| `public` | `Boolean` | Visibilidad del evento |
| `created_at` | `Timestamp` | Fecha de creación formateada |
| `actor__id` | `Long` | ID del usuario (aplanado) |
| `actor__login` | `String` | Username de GitHub (aplanado) |

---

##  Instalación y Ejecución

No es necesario instalar Java o Spark localmente. Gracias a Docker, el entorno está listo para usarse:

1. **Clonar el repositorio:**
```bash
git clone https://github.com/alexbyd/etl-api-to-postgres
cd data-engineer

```


2. **Levantar la infraestructura:**
```bash
docker compose up --build

```



El servicio `etl_script` se encargará de procesar las páginas de la API y cerrar automáticamente al finalizar con un código de éxito.

---

##  Análisis de Datos (SQL)

Una vez que el pipeline finaliza, puedes ejecutar estas consultas en PostgreSQL para extraer insights de los datos recolectados:

### 1. Resumen de Actividad por Tipo de Evento

```sql
SELECT
    type,
    COUNT(*) AS total_eventos
FROM github_events_spark
GROUP BY type
ORDER BY total_eventos DESC;

```

### 2. Top 5 Usuarios más Activos

```sql
SELECT
    actor__login,
    COUNT(*) AS cantidad_acciones
FROM github_events_spark
GROUP BY actor__login
ORDER BY cantidad_acciones DESC
LIMIT 5;

```

### 3. Distribución Temporal (Eventos por Hora)

```sql
SELECT
    DATE_TRUNC('hour', created_at) AS hora,
    COUNT(*) AS eventos
FROM github_events_spark
GROUP BY hora
ORDER BY hora ASC;

```

---

##  Detalles de Implementación

* **Network:** El contenedor de Spark se comunica con la base de datos a través de la red interna de Docker usando el host `destination_postgres`.
* **Resiliencia:** El script incluye un sistema de reintentos para esperar a que la base de datos esté lista antes de intentar la conexión JDBC.
* **Paginación:** Capacidad de recorrer hasta 10 páginas de la API (aprox. 300 eventos por corrida).

---
