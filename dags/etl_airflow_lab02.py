# -*- coding: utf-8 -*-
"""ETL_AIRFLOW_LAB01

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1zbzThJ5UooFpNogHf8Dk5nWqxneOLVdj
"""

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

@task
def extract():
    # Retrieve API key and URL template from Airflow Variables
    api_key = Variable.get('vantage_api_key')
    url_template = Variable.get("Alpha_url")

    # Define the symbols
    symbols = ['ISRG', 'NFLX']  # List of symbols to process

    # Initialize results list
    results = []

    for symbol in symbols:
        # Format the URL with the desired symbol and API key
        url = url_template.format(symbol=symbol, vantage_api_key=api_key)
        print(f"Requesting data for {symbol} from URL: {url}")  # Log the request URL

        # Make the API request
        response = requests.get(url)

        # Check for a successful response
        if response.status_code == 200:
            data = response.json()
            print(f"Response for {symbol}: {data}")  # Log the raw response for debugging

            # Check if the expected data exists in the response
            if "Time Series (Daily)" in data:
                # Extract and process data for the current symbol
                for date, daily_info in data["Time Series (Daily)"].items():
                    daily_info['6. date'] = date  # Add date field
                    daily_info['7. symbol'] = symbol  # Add symbol field
                    results.append(daily_info)
            else:
                print(f"Unexpected response structure for {symbol}: {data}")
        else:
            print(f"Failed to fetch data for {symbol}. Status code: {response.status_code}")

    return results[-180:]  # Return the last 180 records after processing all symbols

@task
def transform(data):
    transformed_results = []
    for record in data:
        transformed = {
            'symbol': record['7. symbol'],
            'date': record['6. date'],
            'open': record['1. open'],
            'high': record['2. high'],
            'low': record['3. low'],
            'close': record['4. close'],
            'volume': record['5. volume']
        }
        transformed_results.append(transformed)
    return transformed_results

@task
def load(transformed_data):
    target_table = "DEV.RAW_DATA.LAB2"

    if not transformed_data:
        print("No data to load.")
        return

    conn = None  # Initialize connection variable
    cur = None   # Initialize cursor variable

    try:
        conn = return_snowflake_conn()  # Establish Snowflake connection
        cur = conn.cursor()  # Create a cursor
        cur.execute("BEGIN;")

        # Create or replace the target table in Snowflake
        cur.execute(f"""
        CREATE OR REPLACE TABLE {target_table} (
          symbol VARCHAR,
          date DATE,
          open NUMBER,
          high NUMBER,
          low NUMBER,
          close NUMBER,
          volume NUMBER
        )
        """)

        # Insert with idempotency check
        insert_sql = f"""
        INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
        SELECT %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM {target_table} WHERE date = %s AND symbol = %s
        )
        """
        
        # Loop over transformed data and insert records into the table
        for record in transformed_data:
            cur.execute(insert_sql, (
                record['symbol'],
                record['date'],
                record['open'],
                record['high'],
                record['low'],
                record['close'],
                record['volume'],
                record['date'],   # For the NOT EXISTS clause
                record['symbol']  # For the NOT EXISTS clause
            ))

        cur.execute("COMMIT;")
        print("Data loaded successfully.")

    except Exception as e:
        if cur:  # Check if cursor was initialized
            cur.execute("ROLLBACK;")
        print("An error occurred while loading data:", str(e))
        raise e
    finally:
        if cur:  # Ensure cursor is closed only if it was created
            cur.close()
        if conn:  # Ensure connection is closed only if it was created
            conn.close()

# Define the DAG
with DAG(
    dag_id='LAB2',
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=['ETL'],
    schedule_interval='30 4 * * *'  # Run at 4:30 AM every day
) as dag:

    # Define the task execution order: extract -> transform -> load
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)
