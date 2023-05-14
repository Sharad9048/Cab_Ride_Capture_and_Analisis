from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

# Here starting a spark session.
spark=SparkSession.builder\
.appName('Kafka_to_Local')\
.getOrCreate()

# The code will read data from the kafka topic 'de-capstone3'
# The format is for declaring that the code will read from a kafka server.
# In the bootstrap server option the private IP address and port is passed.
# In the subcribe option the topic name 'real-time-project' is passed.
# In the starting offset opthin I want the earlist to the latest data within processing time interval.
rawDf = spark.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers"," 18.211.252.152:9092")\
.option("subscribe","de-capstone3")\
.option('startingOffsets','earliest')\
.load()

# The binary data in the value column is converted to string and stored in clickStreamDf dataframe
clickStreamDf = rawDf.select(col('value').cast(StringType()))

# clickStreamQuery query is created to store the data in CSV format in local file system
# The query is executed only once
clickStreamQuery = clickStreamDf\
.writeStream\
.format('csv')\
.option('header','true')\
.option('path','file:///home/hadoop/rawClickStream')\
.option('checkpointLocation','/user/root/clickstream_raw/clickstream_raw_v1')\
.trigger(once=True)\
.start()

clickStreamQuery.awaitTermination()