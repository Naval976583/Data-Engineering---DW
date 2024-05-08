# Updated code JSON to Parquet

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, date_format
import sys
import requests
import json
import logging
import time
from urllib.parse import urlencode

logging.captureWarnings(True)


def get_file(hdfs_file_path, target_location):
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.json(hdfs_file_path)

    output_folder = target_location

    df.write.save(output_folder, format = 'parquet', mode = 'append')
    # Display the contents of the DataFrame
    df.show()

get_file()

# Function to process JSON file and save as Parquet
def process_json(json_file_path, output_folder):   
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("multiline", "true").json("timestamp_testing/input_json/employees_1_testing.json")
    exploded_df = df \
    .select(col(""), col(""), col("")) #Put the column names of the json data

    #If you want to see the converted data
    exploded_df.show()

    exploded_df.write.parquet('t1.parquet')  

process_json()

def combine_parquet_files(input_folder="parquet_files",  # Input folder for merging all the parquet files
                            output_file="combined.parquet"):  # folder in which combined file will be stored
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("mergeSchema", "true").parquet(input_folder)
    df.write.mode('overwrite').parquet(output_file)
    print(f"Combined Parquet file written to: {output_file}")

combine_parquet_files()

def timestamp_format():
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet("combined.parquet/confirm.parquet")  # Combined parquet file path
    if df.count() > 0:
        sorted_df = df.orderBy(desc("timestamp"))
        sorted_df = sorted_df.withColumn("formatted_timestamp",
                                            date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SS'Z'"))
        last_uploaded_data = sorted_df.select("*").first()
        formatted_timestamp = last_uploaded_data.formatted_timestamp
        print("Formatted Timestamp:", formatted_timestamp)
    else:
        print("Last Uploaded Data: <empty>")
    spark.stop()

timestamp_format()

def put_file(hdfs_file_path, target_location):
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.option("multiline", "true").parquet(target_location)

    output_folder = hdfs_file_path

    df.write.save(output_folder, format = 'parquet', mode = 'append')

    # Display the contents of the DataFrame
    df.show()

put_file()

def upsert():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    parq_path = "parq_dir"
    df = spark.read.parquet(parq_path)
    df.createOrReplaceTempView('data')
    update_query = "INSERT INTO data (user_id) VALUES (50)"
    spark.sql(update_query)
    output_parquet_path = "output.parquet"
    df.write.mode('overwrite').parquet(output_parquet_path)

upsert()

""""
Need to specify the column name which we want to update 
MERGE INTO data AS t
USING data2 AS s
ON t.user_id = s.user_id
    AND t.rec_id = s.rec_id
    AND t.uut = s.uut
    AND t.hash_x = s.hash_x
WHEN MATCHED THEN
    UPDATE SET
        t.offer = s.offer,
        t.name = s.name,
        t.hash_y = s.hash_y,
        t.qualified = s.qualified,
        t.rules = s.rules,
WHEN NOT MATCHED THEN
    INSERT (user_id, rec_id, uut, hash_x, offer, name, hash_y, qualified, rules)
    VALUES (s.user_id, s.rec_id, s.uut, s.hash_x, s.offer, s.name, s.hash_y, s.qualified, s.rules);
"""

test_api_url = "https://github.com/Naval976583?tab=projects"  # Update the API endpoint as needed


# function to obtain a new OAuth 2.0 token from the authentication server
def get_new_token():
    auth_server_url = "https://github.com/login/oauth/authorize"
    token_url = "https://github.com/login/oauth/access_token"
    client_id = 'e013d79703ce62d4d159'
    client_secret = '42a49d34290ebff54dd6d407f8e0afc9013cd1cd'
    redirect_uri = 'https://github.com/Naval976583'  # Set the redirect URI as per your GitHub OAuth App settings

    # Construct the authorization URL
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'scope': 'user',  # Adjust the scope as needed
        'state': 'your_state'  # Optional but recommended for security
    }
    auth_url = auth_server_url + '?' + urlencode(params)

    print("Please visit the following URL and authorize the application:")
    print(auth_url)
    authorization_code = input("Enter the authorization code: ")

    # Exchange the authorization code for an access token
    token_req_payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'code': authorization_code,
        'redirect_uri': redirect_uri,
        'state': 'your_state'  # Optional but recommended for security
    }

    token_response = requests.post(token_url, data=token_req_payload, verify=False)
    if token_response.status_code != 200:
        print("Failed to obtain token from the OAuth 2.0 server", file=sys.stderr)
        sys.exit(1)

    print("Successfully obtained a new token")
    # tokens = json.loads(token_response.text)
    access_token = token_response.text.split("=")[1]
    return access_token


# obtain a token before calling the API for the first time


# api_url -> put the url of api in this key
# location_of_table_to_store -> put the file path of table to store here
api_to_table_mapping = {
    '{base_url}/v2/CARCDP/CDP_Tags/?$skip={n}': 'location_of_table_to_store'
}

token = get_new_token()
while True:
    api_call_headers = {'Authorization': 'Bearer ' + token, 'Accept': '*/*', 'Accept-Encoding': 'gzip,deflate,br',
                        'Connection': 'keep-alive',
                        'SPFConfigUID': 'PL_Canmore',
                        'SPFIgnoreConfig': 'true', 'SPFIgnoreEffectivity': 'true', 'Prefer': 'odata.maxpagesize=10'
                        }
    i = 1
    for api_url in api_to_table_mapping:
        skip_limit = 500  # set this limit as per your requirement
        base_url = ''  # replace with actual base_url
        for n in range(100, skip_limit + 1, 100):
            api_call_response = requests.get(api_url.format(base_url, n),
                                             headers=api_call_headers)  # replace test_api_url with api and store api_call_response as json in api key value location
            with open(f"data{i}.json", "w") as f:
                json.dump(api_call_response, f)
            if api_call_response.status_code == 401:
                token = get_new_token()
                print("Invalid Token")
                sys.exit(1)
            else:
                process_json()
                print("Successfully Executed")

        i += 1
        time.sleep(30)
