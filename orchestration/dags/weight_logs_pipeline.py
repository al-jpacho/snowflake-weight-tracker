from notion_client import Client
import os
import pandas as pd
import snowflake.connector
from datetime import timedelta

NOTION = Client(auth=os.environ["NOTION_TOKEN"])
database_id = os.environ["NOTION_DATABASE_ID"]


def query_notion_database(database_id):
    results = []
    has_more = True
    start_cursor = None

    while has_more:
        response = NOTION.databases.query(
            database_id=database_id,
            start_cursor=start_cursor
        )
        results.extend(response["results"])
        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor", None)

    return results


def extract_rows(results): 
    rows = []
    for page in results: 
        props = page["properties"]
        row = {
            "id": page["id"],
            "date": props.get("Date", {}).get("date", {}).get("start", None),
            "weight": props.get("Weight", {}).get("number", None),
            "time_of_day": props.get("Time of Day", {}).get("select", {}).get("name", None),
        }
        rows.append(row)

    return pd.DataFrame(rows)
