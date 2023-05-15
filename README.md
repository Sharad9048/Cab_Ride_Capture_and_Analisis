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

- The project is divided in four parts
1. Extraction
2. Transformation
3. Loading
4. Analysis

## Extraction
- 
