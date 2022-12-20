## Data Pipeline: Stocks Value

Data pipeline (**ETL**), developed in **Python**, scheduled with **Airflow** and run over **docker-compose**, that logs the daily price of different stocks.

The DAG runs data model migrations, fetches data from API (https://www.alphavantage.co/), apply some transformations to the extracted data (filters for date, type casting, rename of coloumns, etc) and stores this in a **Postgres** database. Finally, plot data with **mplfinance**.


##### Set stocks:
./dags/stocks_dag.py

STOCKS = {'company': 'SYMBOL'}


##### Run:
docker-compose up


##### Admin:
http://localhost:8080/home

user: airflow

pass: airflow


##### Postgres:
psql -h 127.0.0.1 -p 5432 -U airflow -d stocks

pass: airflow


##### Plots:
./reports

