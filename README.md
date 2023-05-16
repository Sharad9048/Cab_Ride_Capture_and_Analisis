# Cab_Ride_Capture_and_Analisis
- The data for the analysis id available in Kafka broker and RDS server.
The is extracted from the sources and cleaned with the help of Pyspark code.
The data can be stored in hdfs directly using the code to suit your needs, but in this project the data is saved in CSV format in local file system.
The data in the local fiel system is uploaded to hdfs.
- Hive tables are created to store for the analysis of the data. The data is loaded into hive tables.<br>
The problem statements are as follows:
1. Calculate the total number of different drivers for each customer.
2. Calculate the total rides taken by each customer.
3. Find the total visits made by each customer on the booking page and the total ‘Book Now’ button presses. This can show the conversion ratio.The booking page id is ‘e7bc5fb2-1231-11eb-adc1-0242ac120002’. The Book Now button id is ‘fcba68aa-1231-11eb-adc1-0242ac120002’. You also need to calculate the conversion ratio as part of this task. Conversion ratio can be calculated as Total 'Book Now' Button Press/Total Visits made by customer on the booking page.
4. Calculate the count of all trips done on black cabs.
5. Calculate the total amount of tips given date wise to all drivers by customers.
6. Calculate the total count of all the bookings with ratings lower than 2 as given by customers in a particular month.
7. Calculate the count of total iOS users.

In this project we are using AWS EMR service:
- EMR version-5.36.0
- Hadoop 2.10.1
- Hive 2.3.9
- Sqoop 1.4.7
- Spark 2.4.8

- The project is divided in four parts.
1. Extraction
2. Transformation
3. Model Creation
4. Loading
5. Analysis

## Extraction
- Extracting data from RDS database using sqoop command.
- The sqoop command is in file [ingest.sh](https://github.com/Sharad9048/Cab_Ride_Capture_and_Analisis/blob/master/ingest.sh).
- Download and setup MySQL connector using the following commands.
```
wget https://de-mysql-connector.s3.amazonaws.com/mysql-connector-java-8.0.25.tar.gz
tar -xvf mysql-connector-java-8.0.25.tar.gz
sudo cp mysql-connector-java-8.0.25/mysql-connector-java-8.0.25.jar /usr/lib/sqoop/lib/
```
- Add the following parameters in the file.
  - RDS database public URL
  - table name
  - username
  - password
  - Target directory where data has to be saved in hdfs
  - mappers
- Run the file using following command:
```
bash ingest.sh
```
- Extracting data from Kafka broker using Pyspark code [spark_kafka_to_local.py](https://github.com/Sharad9048/Cab_Ride_Capture_and_Analisis/blob/master/spark_kafka_to_local.py).
- Run the following command to run the Pyspark code:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 spark_kafka_to_local.py
```
## Transformation
- The CSV file will be saved in local file system /home/hadoop/rawClickStream directory.
- For cleaning of the CSV data execute [spark_local_flatten.py](https://github.com/Sharad9048/Cab_Ride_Capture_and_Analisis/blob/master/spark_local_flatten.py) file which also a Pyspark code.
- Run the following command to execute:
```
spark-submit spark_local_flatten.py
```
- The CSV file will be saved in local file system /home/hadoop/clickStream directory.

## Model Creation
- The booking table from RDS database description is as follows.
  - <b>booking_id</b>: Booking ID String.
  - <b>customer_id</b>: Customer ID Number.
  - <b>driver_id</b>: Driver ID Number.
  - <b>customer_app_version</b>: Customer App Version String.
  - <b>customer_phone_os_version</b>: Customer mobile operating system.
  - <b>pickup_lat</b>: Pickup latitude.
  - <b>pickup_lon</b>: Pickup longitude..
  - <b>drop_lat</b>: Dropoff latitude.
  - <b>drop_lon</b>: Dropoff longitude.
  - <b>pickup_timestamp</b>: Timestamp at which customer was picked up.
  - <b>drop_timestamp</b>: Timestamp at which customer was dropped at destination.
  - <b>trip_fare</b>: Total amount of the trip.
  - <b>tip_amount</b>: Tip amount given by customer to driver for this ride.
  - <b>currency_code</b>: Currency Code String for the amount paid by customer.
  - <b>cab_color</b>: Color of the cab.
  - <b>cab_registration_no</b>: Registration number string of the vehicle.
  - <b>customer_rating_by_driver</b>: Rating number given by driver to customer after ride.
  - <b>rating_by_customer</b>: Rating number given by customer to driver after ride.
  - <b>passenger_count</b>: Total count of passengers who boarded the cab for ride.
- The description of the click stream data in Kafka broker is as follows.
  - <b>customer_id</b>: Customer ID Number.
  - <b>app_version</b>: Customer App Version String.
  - <b>os_version</b>: User mobile operating system.
  - <b>lat</b>: Latitude.
  - <b>lon</b>: Longitude.
  - <b>page_id</b>: UUID of the page/screen browsed by a user.
  - <b>button_id</b>: UUID of the button clicked by a user.
  - <b>is_button_click</b>: Yes/No depending on whether a user clicked button.
  - <b>is_page_view</b>: Yes/No depending on whether a user loaded a new screen/page.
  - <b>is_scroll_up</b>: Yes/No depending on whether a user scrolled up on the current screen.
  - <b>is_scroll_down</b>: Yes/No depending on whether a user scrolled down on the current screen.
  - <b>timestamp</b>: Timestamp at which the user action is captured.
- The steps to create Hive tabls are as follows.
  1. Open hive by runn in the following command in shell.
    ```
    hive
    ```
  2. For creating runt he following command in hive console.
    ```
    create table if not exists bookings ( 
    booking_id string, 
    customer_id int, 
    driver_id int, 
    customer_app_version string, 
    customer_phone_os_version string, 
    pickup_lat float, 
    pickup_lon float, 
    drop_lat float, 
    drop_lon float, 
    pickup_timestamp timestamp, 
    drop_timestamp timestamp, 
    trip_fare int, 
    tip_amount int, 
    currency_code string, 
    cab_color string, 
    cab_registration_no string, 
    customer_rating_by_driver tinyint, 
    rating_by_customer tinyint, 
    passenger_count tinyint 
    )
    row format delimited fields terminated by ',' 
    lines terminated by '\n' 
    stored as textfile;
    ```
  3. For creating clickstream table run the following command in hive connsole.
  ```
  create table if not exists clickstream ( 
  customer_id int, 
  app_version string, 
  os_version string, 
  lat float, 
  lon float, 
  page_id string, 
  button_id string, 
  is_button_click string, 
  is_page_view string, 
  is_scroll_up string, 
  is_scroll_down string,
  time_stamp timestamp 
  ) 
  row format delimited fields terminated by ',' 
  lines terminated by '\n' 
  stored as textfile;
  ```
## Loading
- We need to put the CSV data to the hdfs for the hive to access the data.
- Execute the following command.
```
hadoop fs -put /home/hadoop/clickStream /user/root 
```
- To view the CSV file in hsfs run the following command.
```
hadoop fs -cat /user/root/clickStream/*.csv | head -5
```
- Command to load the booking data in hive table is given bellow.
```
load data inpath '/path/to/booking/directory/part-m-00000' into table bookings;
```
Change the location of the command before execution of the code.
- Command to load clickstream data in hive is given bellow.
```
load data inpath '/user/root/bookings/part-m-00000' into table bookings;
```
## Analysis
- The queries of the analysis is available in the [cabride_analysis.sql](https://github.com/Sharad9048/Cab_Ride_Capture_and_Analisis/blob/master/cabride_analysis.sql).
