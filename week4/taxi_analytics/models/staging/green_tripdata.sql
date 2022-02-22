{% set dates = [ "2019_01", "2019_02", "2019_03", "2019_04", "2019_05", "2019_06", "2019_07", "2019_08", "2019_09", "2019_10", "2019_11", "2019_12", "2020_01", "2020_02", "2020_03", "2020_04", "2020_05", "2020_06", "2020_07", "2020_08", "2020_09", "2020_10", "2020_11", "2020_12", ] %}
{% for date in dates %}
SELECT
    "index" as index,
    "VendorID" as vendorid,
    "lpep_pickup_datetime" as lpep_pickup_datetime,
    "lpep_dropoff_datetime" as lpep_dropoff_datetime,
    "store_and_fwd_flag" as store_and_fwd_flag,
    "RatecodeID" as ratecodeid,
    "PULocationID" as pulocationid,
    "DOLocationID" as dolocationid,
    "passenger_count" as passenger_count,
    "trip_distance" as trip_distance,
    "fare_amount" as fare_amount,
    "extra" as extra,
    "mta_tax" as mta_tax,
    "tip_amount" as tip_amount,
    "tolls_amount" as tolls_amount,
    "ehail_fee" as ehail_fee,
    "improvement_surcharge" as improvement_surcharge,
    "total_amount" as total_amount,
    "payment_type" as payment_type,
    "trip_type" as trip_type,
    "congestion_surcharge" as congestion_surcharge
FROM
    {{ source(
        'staging',
        'green_taxi_' + date
    ) }}

    {% if loop.index <= 23 %}
    UNION ALL
    {% endif %}
{% endfor %}

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

