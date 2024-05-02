# Updated code JSON to Parquet

import os
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, date_format

# Function to process JSON file and save as Parquet
def convert_json_to_parquet():
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
    sorted_df = sorted_df.withColumn("formatted_timestamp", date_format("timestamp_column", "yyyy-MM-dd'T'HH:mm:ss.SS'Z'"))
    
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
