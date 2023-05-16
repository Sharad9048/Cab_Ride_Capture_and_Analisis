# Calculate the total number of different drivers for each customer.
select customer_id,count(distinct driver_id) as driver_count from bookings group by 
customer_id;


# Calculate the total rides taken by each customer.
select customer_id,count(booking_id) from bookings group by customer_id; 

# Find the total visits made by each customer on the booking page and the total ‘Book Now’ button presses. 
# This can show the conversion ratio.The booking page id is ‘e7bc5fb2-1231-11eb-adc1-0242ac120002’.
# The Book Now button id is ‘fcba68aa-1231-11eb-adc1-0242ac120002’. You also need to calculate the conversion ratio as part of this task.
# Conversion ratio can be calculated as Total 'Book Now' Button Press/Total Visits made by customer on the booking page.
select visit.customer_id,visit.page_id_count,visit.button_id_count, 
visit.button_id_count/visit.page_id_count as ratio 
from (select customer_id,count(page_id) as page_id_count, 
sum(if(button_id='fcba68aa-1231-11eb-adc1-0242ac120002' and is_button_click='Yes',1,0)) as 
button_id_count 
from clickstream 
where page_id='e7bc5fb2-1231-11eb-adc1-0242ac120002' 
group by customer_id) as visit; 

# Calculate the count of all trips done on black cabs.
select count(*) from bookings where cab_color='black';

# Calculate the total amount of tips given date wise to all drivers by customers.
select to_date(drop_timestamp),sum(tip_amount) 
from bookings group by to_date(drop_timestamp);

# Calculate the total count of all the bookings with ratings lower than 2 as given by customers in a particular month.
select month(drop_timestamp),count(*) 
from bookings 
where rating_by_customer<2 
group by month(drop_timestamp);

# Calculate the count of total iOS users.
select count(distinct customer_id) from bookings 
where customer_phone_os_version='iOS';
