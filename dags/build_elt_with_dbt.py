from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import snowflake.connector

DBT_PROJECT_DIR = "/opt/airflow/lab2"

# Snowflake connection information
conn = BaseHook.get_connection('snowflake_conn')

# Define a function to check idempotency
def check_for_new_data():
    query = """
    SELECT COUNT(*) FROM DEV.RAW_DATA.LAB2 
    WHERE DATE = CURRENT_DATE;
    """
    # Connect to Snowflake
    connection = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get("account"),
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database"),
        schema=conn.schema,
        role=conn.extra_dejson.get("role"),
    )

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result[0] > 0:
            print("Data for today already exists in Snowflake. Skipping dbt run.")
            return False  # No need to run dbt if data is already processed for today
        else:
            print("No data for today found. Proceeding with dbt run.")
            return True
    finally:
        cursor.close()
        connection.close()

# Define the DAG
with DAG(
    "BuildELT_dbt",
    start_date=datetime(2024, 11, 10),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator with idempotency check",
    schedule_interval="0 5 * * *",
    catchup=True,
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:

    # Task to check for new data
    idempotency_check = PythonOperator(
        task_id="check_for_new_data",
        python_callable=check_for_new_data,
    )

    # Only run dbt if idempotency_check returns True (new data exists)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        trigger_rule="all_success",  # Only proceed if idempotency_check succeeded
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        trigger_rule="all_success",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        trigger_rule="all_success",
    )

    # Define task dependencies
    idempotency_check >> dbt_run >> dbt_test >> dbt_snapshot
