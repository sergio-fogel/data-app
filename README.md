# Stocks Value Data App (Python, Postgres, Airflow, Docker-Compose)
Python data application (data pipeline), scheduled with Airflow and run over docker-compose, for stocks value data.
The DAG run the data model migrations, fetches data from API (https://www.alphavantage.co/), apply the extraction logic (filters for date, type casting, transformations, etc), stores the raw daily data (for each execution day) and then insert this in Postgres database. Finally, plot data with mplfinance.


## Set stocks:
./dags/stocks_dag.py
STOCKS = {k:v}

## Run:
docker-compose up

## Admin:
http://localhost:8080/home
user: airflow
pass: airflow

## Postgres:
psql -h 127.0.0.1 -p 5432 -U airflow -d stocks
pass: airflow

## Raw data (.json):
./raw_data

## Plots:
./reports

