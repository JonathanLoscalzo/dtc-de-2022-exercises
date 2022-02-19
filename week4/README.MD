# Week 4

## Prepare the environment Cloud
<details>
  <summary>not working for me</summary>
  
1. using google big query now: 

```
terraform apply # from week1
```

2. execute web_to_gcs.py on google-colab!
more speed, space, and better than my machine.
Another option could be edit web_to_gcs to transform and upload my data.


3. execute gcs_to_bq_dag.py from week3
3. 1. Or create information manually, for example: 

Create external tables tables on gcp

```
-- CREATE EXTERNAL_TABLES
CREATE OR REPLACE EXTERNAL TABLE trips_data_all.yellow_tripdata_external_table
OPTIONS (
  format = 'parquet',
  uris = [
      'gs://dtc_data_lake_dtc-de-jloscalzo/yellow/yellow_tripdata_2019-*.parquet', 
      'gs://dtc_data_lake_dtc-de-jloscalzo/yellow/yellow_tripdata_2020-*.parquet'
    ]
);


CREATE OR REPLACE EXTERNAL TABLE trips_data_all.green_tripdata_external_table
OPTIONS (
  format = 'parquet',
  uris = [
      'gs://dtc_data_lake_dtc-de-jloscalzo/green/green_tripdata_2019-*.parquet', 
      'gs://dtc_data_lake_dtc-de-jloscalzo/green/green_tripdata_2020-*.parquet'
    ]
);

--CREATE PARTITION TABLES

CREATE OR REPLACE TABLE trips_data_all.green_tripdata 
PARTITION BY DATE(lpep_pickup_datetime) 
AS SELECT * FROM trips_data_all.green_tripdata_external_table;

CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata 
PARTITION BY DATE(tpep_pickup_datetime) 
AS SELECT * FROM trips_data_all.yellow_tripdata_external_table;

```

</details>


