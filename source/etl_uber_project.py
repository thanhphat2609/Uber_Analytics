# Import needed Libraries

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, row_number, udf

# Import module
from extract import extract_data
from transform import transform_data


# Create Spark session
spark = SparkSession.builder.appName("ET_Spark") \
                            .config('spark.cores.max', "16") \
                            .config("spark.executor.memory", "70g") \
                            .config("spark.driver.memory", "50g") \
                            .config("spark.memory.offHeap.enabled",True) \
                            .config("spark.memory.offHeap.size","16g") \
                            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")\
                            .enableHiveSupport() \
                            .getOrCreate()
                            

# Extract data
hdfs_path = "hdfs://localhost:9000/user/thanhphat/datalake/uberdata/*.csv"
df_uber = extract_data(hdfs_path, spark)

# Drop duplicates data
df_uber = df_uber.dropDuplicates()


# Transformation data

dim_datetime_df, dim_pickup_df, dim_dropoff_df, dim_ratecode_df = transform_data(df_uber, spark)
dim_ratecode_df.show(10)


# Load data


