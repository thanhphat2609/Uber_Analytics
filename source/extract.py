# Define function for extract data from HDFS path
def extract_data(hdfs_path, spark):
	dataframe = spark.read.csv(hdfs_path, header = True, inferSchema = True)
	return dataframe
