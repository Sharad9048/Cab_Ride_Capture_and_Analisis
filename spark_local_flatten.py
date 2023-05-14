from pyspark.sql import SparkSession
from pyspark.sql.functions import col,udf,from_json
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,TimestampType


# Here starting a spark session.
spark = SparkSession\
.builder\
.master('yarn')\
.appName('spark_local_flatten')\
.getOrCreate()

# Reading CSV file having headers.
rawDf = spark.read.csv('file:///home/hadoop/rawClickStream/*.csv',header=True)

# Defining the schema.
schema_stream = StructType([
    StructField('customer_id',StringType(),False),
    StructField('app_version',StringType(),False),
    StructField('OS_version',StringType(),False),
    StructField('lat',StringType(),False),
    StructField('lon',StringType(),False),
    StructField('page_id',StringType(),False),
    StructField('button_id',StringType(),False),
    StructField('is_button_click',StringType(),False),
    StructField('is_page_view',StringType(),False),
    StructField('is_scroll_up',StringType(),False),
    StructField('is_scroll_down',StringType(),False),
    StructField('timestamp',StringType(),False),
])

# Creating UDF for data cleanup.
rep = udf(lambda x:x.replace('\\n',''))

# Converting JSON string to dataframe.
# 1. The data is cleaned using rep UDF.
# 2. The JSON string is converted to dataframe using from_json function and schema defined above.
clickStreamDf = rawDf\
.select(from_json(rep(col("value").cast("string")),schema_stream).alias('data'))\
.select('data.*')

# The dataframe is stored in local sile system in CSV format without headers.
clickStreamDf.write.csv('file:///home/hadoop/clickStream')