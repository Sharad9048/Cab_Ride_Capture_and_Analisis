import threading,os,time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType,LongType
from pyspark.sql.functions import col   ,from_json

from pyspark.streaming.context import StreamingContext

# """
# This function is for stopping the query gracefully when 'stop' file is found
# """
def stopQueryProcess(query:StreamingContext,stopFile):
    while(1):
        if os.path.exists(stopFile):
            print("Stoping command recieved to stop {}".format(query.name))
            while query.isActive:
                if not query.status['isTriggerActive']:
                    print("Trigger is in Inactive stopping query {}".format(query.name))
                    query.stop()
                    break
                else:
                    print("Trigger is active for {}".format(query.name))
                    time.sleep(0.5)
            break
        time.sleep(1)

# """
# This function is for starting a thread to stop the query.
# """
def stopQuery(query:StreamingContext):
    print("-----------------------------------------------------------------------------------------")
    print("Run command 'touch stop' to stop the spark streaming {}".format(query.name))
    stopFile = 'stop'
    stopProcess = threading.Thread(target=stopQueryProcess,args=(query,stopFile,),daemon=True)
    stopProcess.setDaemon(True)
    stopProcess.start()


spark=SparkSession.builder\
.appName('Capstone')\
.getOrCreate()

rawDf = spark.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers"," 18.211.252.152:9092")\
.option("subscribe","de-capstone3")\
.option('startingOffsets','earliest')\
.load()

schema = StructType([
    StructField('customer_id',LongType(),False),
    StructField('app_version',StringType(),False),
    StructField('OS_version',StringType(),False),
    StructField('lat',DoubleType(),False),
    StructField('lon',DoubleType(),False),
    StructField('page_id',StringType(),False),
    StructField('button_id',StringType(),False),
    StructField('is_button_click',StringType(),False),
    StructField('is_page_view',StringType(),False),
    StructField('is_scroll_up',StringType(),False),
    StructField('is_scroll_down',StringType(),False),
    StructField('timestamp',TimestampType(),False),
])

clickStreamDf = rawDf.select(from_json(col('value').cast('string'),schema))
# .select(from_json(col("value").cast("string"),schema).alias('data'))\
# .select('data.*')



clickStreamQuery = clickStreamDf\
.writeStream\
.format('console')\
.option('truncate','false')\
.trigger(processingTime='1 minutes')\
.start()
# .format('json')\
# .outputMode('append')\
# .option('path','/user/root/clickstream_raw/')\
# .option('checkpointLocation','/user/root/clickstream_raw/clickstream_raw_v1/')\
# .trigger(processingTime='1 minutes')\
# .start()

stopQuery(clickStreamQuery)

clickStreamQuery.awaitTermination()