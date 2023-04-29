sqoop import \
--connect jdbc:mysql://upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com/testdatabase \
--table bookings \
--username student --password STUDENT123 \
--fields-terminated-by ',' \
--lines-terminated-by '\n' \
--target-dir /user/root/cabride_raw \
-m 1