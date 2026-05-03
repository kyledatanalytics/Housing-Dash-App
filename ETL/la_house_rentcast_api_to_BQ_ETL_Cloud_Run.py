import requests
import json
import os
import pandas as pd
import pandas_gbq 
from google.cloud import bigquery
from google.oauth2 import service_account

#CGP Configuration 
service_account_path = 'path/to/your/service_account_key.json'
project_id = 'housing-app-494519'
dataset_id = 'la_housing'
target_table = f"{project_id}.{dataset_id}.sales_listing"
staging_table = f"{project_id}.{dataset_id}.sales_listing_staging"
client = bigquery.Client(project=project_id)

PAGE_SIZE = 500  # RentCast max rows per request


def rentcast_api_call(url, params, row_extract):
    """
    Fetch up to `row_extract` rows from RentCast, paging in steps of 500.
    Offsets are 0, 500, 1000, ...; each request uses limit=min(500, remaining).
    """
    api_key = os.getenv("RENTCAST_API_KEY")
    if not api_key:
        raise ValueError("Missing RENTCAST_API_KEY environment variable")

    headers = {
        "Accept": "application/json",
        "X-Api-Key": api_key,
    }

    all_rows = []
    offset = 0

    while offset < row_extract:
        remaining = row_extract - offset
        batch_limit = min(PAGE_SIZE, remaining)
        request_params = {**params, "limit": batch_limit, "offset": offset}
        response = requests.get(url, params=request_params, headers=headers)

        if response.status_code != 200:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            print(response.text)
            raise RuntimeError(f"RentCast API error: {response.status_code}")

        data = response.json()
        if not data:
            break
        if not isinstance(data, list):
            raise TypeError(
                f"Expected a JSON list from RentCast, got {type(data).__name__}"
            )

        all_rows.extend(data)

        if len(data) < batch_limit:
            break

        offset += PAGE_SIZE

    df = pd.DataFrame(all_rows)
    if len(df) > row_extract:
        df = df.iloc[:row_extract].copy()

    print("Success! Here is the DataFrame preview:")
    print(df.head())
    return df


# Example: pull sales listings (adjust row_extract as needed)
url = "https://api.rentcast.io/v1/listings/sale"
params = {
    "city": "Los Angeles",
    "state": "CA",
}
df_sales = rentcast_api_call(url, params, row_extract=1000)


df_sales['history'] = df_sales['history'].apply(lambda x: json.dumps(x) if x is not None else None)
df_sales['builder'] = df_sales['builder'].apply(lambda x: json.dumps(x) if x is not None else None)
df_sales["date_update"] = pd.Timestamp.now(tz="America/Los_Angeles").floor("us")

#upload staging table to BQ 
try:
    pandas_gbq.to_gbq(
        df_sales, 
        destination_table=staging_table, 
        project_id=project_id, 
        if_exists='replace' 
    )
    print(f"Data uploaded successfully to {staging_table}")
except Exception as e:
    print(f"Error uploading data: {e}")

cols = [f"`{col}`" for col in df_sales.columns]
update_stmt = ", ".join([f"T.{c} = S.{c}" for c in cols if c != '`id`'])
insert_cols = ", ".join(cols)
insert_values = ", ".join([f"S.{c}" for c in cols])

merge_query = f"""
MERGE `{target_table}` T
USING (
  SELECT 
    * EXCEPT(date_update), 
    CAST(date_update AS DATETIME) AS date_update 
  FROM `{staging_table}`
) S
ON T.id = S.id
WHEN MATCHED THEN
  UPDATE SET {update_stmt}
WHEN NOT MATCHED THEN
  INSERT ({insert_cols})
  VALUES ({insert_values})
"""

try:
    merge_job = client.query(merge_query)
    merge_job.result()  # Wait for MERGE to finish.
    inserted_rows = merge_job.dml_stats.inserted_row_count or 0
    print(f"Data merged successfully. New rows inserted: {inserted_rows}")
except Exception as e:
    print(f"Error merging data: {e}")

