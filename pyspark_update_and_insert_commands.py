import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
parq_path = "parq_dir"
df = spark.read.parquet(parq_path)
df.createOrReplaceTempView('data')
update_query = "INSERT INTO data (user_id) VALUES (50)"
spark.sql(update_query)
output_parquet_path = "output.parquet"
df.write.mode('overwrite').parquet(output_parquet_path)