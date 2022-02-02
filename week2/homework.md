
<!-- https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-02.csv -->
<!-- https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -->

## Question 1: Start date for the Yellow taxi data (1 point)

You'll need to parametrize the DAG for processing the yellow taxi data that
we created in the videos. 

What should be the start date for this dag?

* 2019-01-01
* 2020-01-01
* 2021-01-01
* days_ago(1)

A: 2019-01-01 and end_date 2020-01-01

## Question 2: Frequency for the Yellow taxi data (1 point)

How often do we need to run this DAG?

* Daily
* Monthly
* Yearly
* Once

A: monthly

## Question 3: DAG for FHV Data (2 points)

Question: how many DAG runs are green for data in 2019 after finishing everything? 

Note: when processing the data for 2020-01 you probably will get an error. It's up 
to you to decide what to do with it - for Week 3 homework we won't need 2020 data.

A: 24

## Question 4: DAG for Zones (2 points)


Create the final DAG - for Zones:

* Download it
* Parquetize 
* Upload to GCS

(Or two steps for local ingestion: download -> ingest to postgres)

How often does it need to run?

* Daily
* Monthly
* Yearly
* Once

A: Once (depending on when zones changes, but they don't usually change)


