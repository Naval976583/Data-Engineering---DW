# Updated code JSON to Parquet

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, date_format
import sys
import requests
import json
import logging
import time
from urllib.parse import urlencode

logging.captureWarnings(True)


# Function to process JSON file and save as Parquet
def convert_json_to_parquet():a
    def process_json(json_file_path, output_folder):
        # Read the JSON data from the file
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        # Flatten the JSON data using json_normalize, adding a prefix to metadata columns
        flattened_data = pd.json_normalize(data, record_path=['qualify'],
                                           meta=['user_id', 'rec_id', 'uut'])

        rules_data = pd.json_normalize(data, record_path=['qualify', 'rules'])

        flattened_data = pd.concat([flattened_data, rules_data], axis=1)

        flattened_data.drop('rules', axis=1, inplace=True)

        # Determine the output folder for Parquet file
        output_folder_path = os.path.join(output_folder, "parquet_files")
        os.makedirs(output_folder_path, exist_ok=True)

        # Determine the output path for Parquet file
        parquet_output_path = os.path.join(output_folder_path,
                                           os.path.splitext(os.path.basename(json_file_path))[0] + '.parquet')

        # Save the DataFrame as Parquet
        flattened_data.to_parquet(parquet_output_path, index=False)

        print(f"Processed: {json_file_path} -> Saved as: {parquet_output_path}")

    # Directory containing JSON files
    json_files_directory = "kamalyesh-test/input"

    # Output directory for Parquet files
    output_folder = "kamalyesh-test/parq_dir"

    # Iterate over each JSON file in the directory
    for file_name in os.listdir(json_files_directory):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(json_files_directory, file_name)
            process_json(json_file_path, output_folder)

    # Function to combine Parquet files into one
    def combine_parquet_files(input_folder="kamalyesh-test/parq_dir/parquet_files",
                              output_file="kamalyesh-test/parq_dir/output/combined.parquet"):
        spark = SparkSession.builder.getOrCreate()
        # Read all Parquet files from the input folder
        df = spark.read.option("mergeSchema", "true").parquet(input_folder)

        # Write the combined DataFrame to a single Parquet file
        df.write.mode('overwrite').parquet(output_file)

        print(f"Combined Parquet file written to: {output_file}")

    # Example usage
    combine_parquet_files()


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Fetch Last Uploaded Data from Parquet Table") \
    .getOrCreate()

df = spark.read.parquet("kamalyesh-test/parq_dir/output/combined parquet.parquet")

# Check if the DataFrame is empty
if df.count() > 0:
    # Sort the DataFrame by timestamp column in descending order
    sorted_df = df.orderBy(desc("timestamp_column"))

    # Format the timestamp column to "yyyy-MM-dd'T'HH:mm:ss.SS'Z'" format
    sorted_df = sorted_df.withColumn("formatted_timestamp",
                                     date_format("timestamp_column", "yyyy-MM-dd'T'HH:mm:ss.SS'Z'"))

    # Get the first row to fetch the last uploaded data
    last_uploaded_data = sorted_df.select("*").first()

    # Retrieve only the formatted timestamp
    formatted_timestamp = last_uploaded_data.formatted_timestamp

    # Print the formatted timestamp
    print("Formatted Timestamp:", formatted_timestamp)
else:
    # If DataFrame is empty, print an empty string
    print("Last Uploaded Data: <empty>")

# Stop the SparkSession
spark.stop()

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
                convert_json_to_parquet()
                print("Successfully Executed")

        i += 1
        time.sleep(30)
