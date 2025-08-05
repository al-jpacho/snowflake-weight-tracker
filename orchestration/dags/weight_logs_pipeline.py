import os
from datetime import datetime, timedelta

import pandas as pd
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv
from notion_client import Client

load_dotenv(dotenv_path="/opt/airflow/.env")

# Initialise Notion client
NOTION = Client(auth=os.getenv("NOTION_TOKEN"))
database_id = os.getenv("NOTION_DATABASE_ID")


def query_notion_database(database_id: str) -> list[dict]:
    """
    Queries a Notion database and retrieves all results.

    Args:
        database_id (str): The ID of the Notion database to query.

    Returns:
        list[dict]: A list of dictionaries containing the results from the database.
    """
    results = []
    has_more = True
    start_cursor = None

    while has_more:
        response = NOTION.databases.query(
            database_id=database_id, start_cursor=start_cursor
        )
        results.extend(response["results"])
        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor", None)

    return results


def extract_rows(results: list[dict]) -> pd.DataFrame:
    """
    Extracts rows of data from a list of dictionaries and converts them into a pandas DataFrame.

    Args:
        results (list[dict]): A list of dictionaries containing page data with properties.

    Returns:
        pd.DataFrame: A DataFrame containing extracted 'id', 'date', 'weight', and 'time_of_day' fields.
    """
    rows = []
    for page in results:
        props = page["properties"]
        row = {
            "id": page["id"],
            "date": props.get("Date", {}).get("date", {}).get("start", None),
            "weight": props.get("Weight", {}).get("number", None),
            "time_of_day": props.get("Time of Day", {})
            .get("select", {})
            .get("name", None),
        }
        rows.append(row)

    return pd.DataFrame(rows)


# Airflow DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weight_logs_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    def pull_from_notion_db_and_insert():
        # Query Notion database
        notion_data = query_notion_database(database_id)
        df_weight_logs = extract_rows(notion_data)

        df_weight_logs["loaded_at"] = pd.to_datetime("now").isoformat()
        df_weight_logs["loaded_at"] = df_weight_logs["loaded_at"].astype(str)

        # Snowflake connection
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )

        cursor = conn.cursor()

        # Write DataFrame to Snowflake
        insert_query = """
            INSERT INTO WEIGHT_DB.RAW.weight_logs_raw_temp (id, date, weight_kg, time_of_day,loaded_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        records = list(
            df_weight_logs[
                ["id", "date", "weight", "time_of_day", "loaded_at"]
            ].itertuples(index=False, name=None)
        )

        cursor.executemany(insert_query, records)

        conn.commit()
        conn.close()

    ingest_task = PythonOperator(
        task_id="pull_from_notion_db_and_insert",
        python_callable=pull_from_notion_db_and_insert,
        dag=dag,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/weight_tracker && dbt run",
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /opt/airflow/weight_tracker && dbt test",
    )

    ingest_task >> run_dbt >> run_dbt_tests
