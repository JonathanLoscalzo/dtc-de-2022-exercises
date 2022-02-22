## Week 4 Homework 
[Form](https://forms.gle/B5CXshja3MRbscVG8) 

We will use all the knowledge learned in this week. Please answer your questions via form above.  
* You can submit your homework multiple times. In this case, only the last submission will be used. 

**Deadline** for the homework is 23rd Feb 2022 22:00 CET.


In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:
* Building a source table: stg_fhv_tripdata
* Building a fact table: fact_fhv_trips
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

### Question 1: 
**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)**  
You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

```
select count(1) from fact_trips ft 
where extract(year from pickup_datetime) between 2019 and 2020

A: 61.584.804
time: 70s
```

### Question 2: 
**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase. 

```
select service_type, extract(year from pickup_datetime) as year, count(1)
from fact_trips ft 
where extract(year from pickup_datetime) between 2019 and 2020
group by service_type, extract(year from pickup_datetime) 

A:
Green	2019.0	5112661   0,083018223 
Green	2020.0	1142442   0,018550713 0.1
Yellow	2019.0	40565647  0,658695723
Yellow	2020.0	14764054  0,239735341 0.9
```

### Question 3: 
**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

```
select count(1)
from stg_fhv_tripdata
where extract(year from pickup_datetime) = 2019 

A: 42.084.899
```

### Question 4: 
**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

```
select count(1)
from fact_fhv_trips
where extract(year from pickup_datetime) = 2019 

A: 22.676.253
```

### Question 5: 
**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

```
select
	extract(month from pickup_datetime) as month_p ,
	extract(year from pickup_datetime) as year_p,
	count(1)
from
	fact_fhv_trips
where
	extract(year from pickup_datetime) between 2019 and 2020
group by 
	extract(month from pickup_datetime), 
	extract(year from pickup_datetime)

month   year    count
1.0 	2019.0	19847263 x
1.0 	2020.0	380307
2.0 	2019.0	187156
2.0 	2020.0	153399
3.0 	2019.0	104954
3.0 	2020.0	266011
4.0 	2019.0	237815
4.0 	2020.0	86813
5.0 	2019.0	242857
5.0 	2020.0	124178
6.0 	2019.0	257519
6.0 	2020.0	168283
7.0 	2019.0	272160
7.0 	2020.0	199944
8.0 	2019.0	300738
8.0 	2020.0	206241
9.0 	2019.0	288208
9.0 	2020.0	230268
10.0	2019.0	328964
10.0	2020.0	235699
11.0	2019.0	267820
11.0	2020.0	214322
12.0	2019.0	340799
12.0	2020.0	170207

A: 2019/1
```