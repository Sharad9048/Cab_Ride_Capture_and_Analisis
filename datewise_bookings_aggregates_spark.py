from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col,count

# Here starting a spark session.
spark = SparkSession.builder.master('yarn').appName('aggregate').getOrCreate()

# RDS Host URL.
url = 'jdbc:mysql://upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com/testdatabase'

# RDS username.
user = 'student'

# RDS password.
password = 'STUDENT123'

# RDS table name.
table = 'bookings'

# Creating a dictionary to read data from RDS database.
# This for selecting the driver and storing username and password.
options = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": user,
    "password": password
}

# Reading data from RDS and storing into a dataframe rawDf.
rawDf = spark.read.jdbc(url=url,table=table,properties=options)

# Performing aggregations for datawise total bookings.
# Coalesce is for merging all the partition created during aggregation.
aggDf = rawDf.groupBy(to_date(col('pickup_timestamp')).alias('date'))\
.agg(
    count('booking_id').alias('total_bookings')
).coalesce(1)

# Storing the aggregated data in CSV format in local file system
aggDf.write.csv('file:///home/hadoop/aggregates')