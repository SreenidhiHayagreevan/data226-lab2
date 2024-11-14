# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector
import requests
from datetime import datetime, timedelta
import yfinance as yf


def get_next_day(date_str):
    """
    Given a date string in 'YYYY-MM-DD' format, returns the next day as a string in the same format.
    """
    # Convert the string date to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    
    # Add one day using timedelta
    next_day = date_obj + timedelta(days=1)
    
    # Convert back to string in "YYYY-MM-DD" format
    return next_day.strftime("%Y-%m-%d")


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


def get_logical_date():
    # Get the current context
    context = get_current_context()
    return str(context['logical_date'])[:10]


@task
def extract(symbol):
    date = get_logical_date()

    # Download the data for the specific date (one day range)
    end_date = get_next_day(date)
    data = yf.download(symbol, start=date, end=end_date)
    # return data
    return data.to_dict(orient="list")


@task
def load(d, symbol, target_table):
    date = get_logical_date()
    cur = return_snowflake_conn()

    try:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            date date, open float, close float, high float, low float, volume int, symbol varchar 
        )""")
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {target_table} WHERE date='{date}'")
        sql = f"""INSERT INTO {target_table} (date, open, close, high, low, volume, symbol) VALUES (
          '{date}', {d['Open'][0]}, {d['Close'][0]}, {d['High'][0]}, {d['Low'][0]}, {d['Volume'][0]}, '{symbol}[0]')"""
        print(sql)
        cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'YfinanceToSnowflake',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "dev.raw_data.stock_price"
    symbol = "AAPL"

    data = extract(symbol)
    load(data, symbol, target_table)
