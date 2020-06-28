# PYTHON-ETL

1. To run the script you need to have Python 3.6 or 3.7 installed

2. Modify the paths of the config.json file
- ORDERS_DOWNLOAD_URL: The downloadable URL of the zip file to be processed
- DOWNLOAD_PATH: Here you will download · the Zip from the web
- EXPORT_PATH: Here unzip the content of the ZIP and then process the data

3. Create the tables in Postgres:
```
CREATE TABLE headers (
page varchar(100), 
start_date date,
end_date date,
DateInsert timestamp
);

CREATE TABLE orders (
id integer,
client_id integer,
client varchar (100),
date timestamp,
discount_price float,
DateInsert timestamp
);

CREATE TABLE order_details (
id integer,
client_id integer,
client varchar (100),
date timestamp,
discount_price float,
DateInsert timestamp,
product_id integer,
color varchar (50),
price float,
units integer
);

CREATE TABLE ctl_activity_process (
etl_master varchar (50),
id_process bigint,
file varchar (100),
activity varchar (100),
status integer,
start_date timestamp,
end_date timestamp,
cant_row integer
);
```

4. YOU ARE READY TO RUN