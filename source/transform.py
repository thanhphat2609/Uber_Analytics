from pyspark.sql.functions import lit, col, when, row_number, udf
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Define function for create Dim_DateTime
def create_dim_datetime(df_base, spark, window):
	# Choose column
	dim_datetime_df = df_base['tpep_pickup_datetime', 'tpep_dropoff_datetime']
	
	# Create column DateTimeID
	dim_datetime_df = dim_datetime_df.withColumn('DateTimeID', row_number().over(window))
	
	# Re-order of column
	dim_datetime_df.createOrReplaceTempView("Dim_DateTime")
	dim_datetime_df = spark.sql(''' 
					SELECT DateTimeID, tpep_pickup_datetime, tpep_dropoff_datetime
					FROM Dim_DateTime
				   ''')
	# Drop Tempview
	spark.catalog.dropTempView("Dim_DateTime")
	
	return dim_datetime_df
	
	
# Define function for create Dim_PickUp
def create_dim_pickup(df_base, spark, window):
	# Choose column
	dim_pickup_df = df_base['pickup_longitude', 'pickup_latitude']
	
	# Create column PickUpID
	dim_pickup_df = dim_pickup_df.withColumn("PickUpID", row_number().over(window))
	
	# Re-order of column
	dim_pickup_df.createOrReplaceTempView("Dim_PickUp")
	dim_pickup_df = spark.sql(''' 
					SELECT PickUpID, pickup_longitude, pickup_latitude
					FROM Dim_Pickup
				 ''')
	# Drop TempView
	spark.catalog.dropTempView("Dim_PickUp")
	
	return dim_pickup_df


# Function for create Dim_DropOff
def create_dim_dropoff(df_base, spark, window):
	# Choose column
	dim_dropoff_df = df_base['dropoff_longitude', 'dropoff_latitude']
	
	# Create column DropOffID
	dim_dropoff_df = dim_dropoff_df.withColumn('DropOffID', row_number().over(window))
	
	# Re-order of column
	dim_dropoff_df.createOrReplaceTempView("Dim_DropOff")
	dim_dropoff_df = spark.sql(''' 
					SELECT DropOFfID, dropoff_longitude, dropoff_latitude
					FROM Dim_DropOff
				  ''')

	# Drop TempView
	spark.catalog.dropTempView("Dim_DropOff")
	
	return dim_dropoff_df

	
# Function for create Dim_RateCode
def create_dim_ratecode(df_base, spark):
	# Choose column
	dim_ratecode_df = df_base[['RateCodeID']]
	
	# Drop duplicates values
	dim_ratecode_df = dim_ratecode_df.dropDuplicates()
	
	# List data with dictionary
	dict_ratecode_type = {
		'1':"Standard rate",
		'2':"JFK",
		'3':"Newark",
		'4':"Nassau or Westchester",
		'5':"Negotiated fare",
		'6':"Group ride"
	}
    	
    	# Convert dict_value to a list of tuples
	data_list = list(dict_ratecode_type.items())

	# Define Schema
	schema = StructType([
	    StructField('RateCodeID', StringType(), True),
	    StructField('rate_code_type', StringType(), True)
	])

	# Convert to DataFrame
	df_dict = spark.createDataFrame(data = data_list, schema = schema)
    	
    	# Create TempView
	dim_ratecode_df.createOrReplaceTempView("Dim_RateCode")
	df_dict.createOrReplaceTempView("Dim_Dict")
    	
    	# Join two tables
	dim_ratecode_df = spark.sql(''' 
					SELECT DC.RateCodeID, DD.rate_code_type
					FROM Dim_RateCode DC join Dim_Dict DD on DC.RateCodeID = DD.RateCodeID
				   ''')
    				   
    	# Drop Temp View
	spark.catalog.dropTempView("Dim_RateCode")
	spark.catalog.dropTempView("Dim_Dict")

	return dim_ratecode_df


# Function for call all Create Dim_Table, Fact_Table
def transform_data(df_base, spark):
	# Make an identiy for create column ID if needed
	window = Window().orderBy((lit('A')))
	
	# Create Dim_DateTime
	dim_datetime_df = create_dim_datetime(df_base, spark, window)
	
	# Create Dim_Pickup
	dim_pickup_df = create_dim_pickup(df_base, spark, window)
	
	# Create Dim_Dropoff
	dim_dropoff_df = create_dim_dropoff(df_base, spark, window)
	
	# Create Dim_RateCode
	dim_ratecode_df = create_dim_ratecode(df_base, spark)
	
	return dim_datetime_df, dim_pickup_df, dim_dropoff_df, dim_ratecode_df
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
