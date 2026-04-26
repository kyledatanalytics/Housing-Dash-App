#%%
import requests
import json
import os
import pandas as pd
import pandas_gbq 
from google.cloud import bigquery
from google.oauth2 import service_account
#%%
#CGP Configuration 
service_account_path = 'path/to/your/service_account_key.json'
project_id = 'housing-app-494519'
dataset_id = 'la_housing'
target_table = f"{project_id}.{dataset_id}.sales_listing"
staging_table = f"{project_id}.{dataset_id}.sales_listing_staging"
client = bigquery.Client(project=project_id)
# %%
#pull sales listing from API and save to staging table
url = "https://api.rentcast.io/v1/listings/sale"
params = {
    "city": "Los Angeles",
    "state": "CA",
    "limit": 1000,
    "offset": 500
}

# 2. Define the headers
# Replace 'YOUR_API_KEY' with your actual RentCast API key
headers = {
    "Accept": "application/json",
    "X-Api-Key": os.getenv("rentcast_api_key") 
}

# 3. Make the GET request
response = requests.get(url, params=params, headers=headers)

# 4. Check if the request was successful
if response.status_code == 200:
    data = response.json()
    
    # 5. Convert JSON results into a pandas DataFrame
    # Note: RentCast usually returns a list of property objects
    df_sales = pd.DataFrame(data)
    
    # Display the first few rows
    print("Success! Here is the DataFrame preview:")
    print(df_sales.head())
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    print(response.text)


# %%
df_sales['history'] = df_sales['history'].apply(lambda x: json.dumps(x) if x is not None else None)
df_sales['builder'] = df_sales['builder'].apply(lambda x: json.dumps(x) if x is not None else None)
#%%
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
#%%
merge_query = f"""
MERGE `{target_table}` T
USING `{staging_table}` S
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



# %%
# try:
#     pandas_gbq.to_gbq(
#         df_sales_target, 
#         destination_table=target_table, 
#         project_id=project_id, 
#         if_exists='replace' 
#     )
#     print(f"Data uploaded successfully to {target_table}")
# except Exception as e:
#     print(f"Error uploading data: {e}")
