

Install Google Cloud SDK. What's the version you have? 

To get the version, run `gcloud --version`

## Google Cloud account 

Create an account in Google Cloud and create a project.

answer: Google Cloud SDK 370.0.0


## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output (after running `apply`) to the form.

It should be the entire output - from the moment you typed `terraform init` to the very end.

## Prepare Postgres 

Run Postgres and load data as shown in the videos

We'll use the yellow taxi trips from January 2021:

```bash
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
```

You will also need the dataset with zones:

```bash 
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

Download this data and put it to Postgres


```
pyenv activate dezoo-env

python ingest_data.py \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--db=ny_taxi \
--table_name=TAXI_ZONE \
--csv-name=./data/taxi+_zone_lookup.csv


python ingest_data.py \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--db=ny_taxi \
--table_name=YELLOW_TAXI_TRIPS \
--csv-name=./data/yellow_tripdata_2021-01.csv
```

## Question 3. Count records 

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

```
select count(*) from "YELLOW_TAXI_TRIPS" ytt
where tpep_pickup_datetime >= '2021-01-15' and tpep_pickup_datetime < '2021-01-16'

select count(*) from "YELLOW_TAXI_TRIPS" ytt
where tpep_pickup_datetime::date = '2021-01-15';

-- A: 53024
```


## Question 4. Largest tip for each day

Find the largest tip for each day. 
On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

```

select pickup_date, max(max_tip)  as max_tip
from (
	select tpep_pickup_datetime::date  as pickup_date, count(1), max(tip_amount) as max_tip
	from "YELLOW_TAXI_TRIPS" ytt 
	group by 1
) as t
where extract(month from t.pickup_date) = 1
group by 1
order by max_tip desc

select tpep_pickup_datetime::date as pickup_date, count(1), max(tip_amount) as max_tip
from "YELLOW_TAXI_TRIPS" ytt 
where extract(month from tpep_pickup_datetime::date ) = 1
group by 1
order by max_tip desc

-- A: 2021-01-20 / 1140,44
```

## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown" 

```
select Coalesce(tz2."Zone", 'Unknown'), count(1) as count_pu
from "YELLOW_TAXI_TRIPS" ytt 
left join "TAXI_ZONE" tz on ytt."PULocationID"  = tz."LocationID" 
left join "TAXI_ZONE" tz2 on ytt."DOLocationID"  = tz2."LocationID"
where tpep_pickup_datetime::date = '2021-01-14' and ytt."PULocationID" = 43
group by 1
order by count_pu desc


select Coalesce(tz2."Zone", 'Unknown') as DO_ZONE, count(1) as count_pu
from "YELLOW_TAXI_TRIPS" ytt 
left join "TAXI_ZONE" tz on ytt."PULocationID"  = tz."LocationID" 
left join "TAXI_ZONE" tz2 on ytt."DOLocationID"  = tz2."LocationID"
where tpep_pickup_datetime::date = '2021-01-14' and tz."Zone"  = 'Central Park'
group by 1
order by count_pu desc

-- A: Upper East Side South	97
```

## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East". 

```
select Coalesce(tz."Zone", 'Unknown') as PU_ZONE, Coalesce(tz2."Zone", 'Unknown') as DO_ZONE, count(1), avg(total_amount)
from "YELLOW_TAXI_TRIPS" ytt 
left join "TAXI_ZONE" tz on ytt."PULocationID"  = tz."LocationID"
left join "TAXI_ZONE" tz2 on ytt."DOLocationID"  = tz2."LocationID"
group by PU_ZONE, DO_ZONE 
order by avg desc

select concat(Coalesce(tz."Zone", 'Unknown'), '/', Coalesce(tz2."Zone", 'Unknown')) as pu_do_zone, count(1), avg(total_amount)
from "YELLOW_TAXI_TRIPS" ytt 
left join "TAXI_ZONE" tz on ytt."PULocationID"  = tz."LocationID"
left join "TAXI_ZONE" tz2 on ytt."DOLocationID"  = tz2."LocationID"
group by 1
order by avg desc

-- A: Alphabet City/Unknown 2292.4
```

## Submitting the solutions

* Form for submitting: https://forms.gle/yGQrkgRdVbiFs8Vd7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Wednesday), 22:00 CET

