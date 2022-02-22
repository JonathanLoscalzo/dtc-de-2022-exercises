{% set dates_2020 = ["2020_01", "2020_02", "2020_03", "2020_04", "2020_05", "2020_06", "2020_07", "2020_08", "2020_09", "2020_10", "2020_11", "2020_12", ] %}
{% for date in dates_2020 %}
SELECT
    "index" AS index,
    "dispatching_base_num" AS dispatching_base_num,
    "pickup_datetime" AS pickup_datetime,
    "dropoff_datetime" AS dropoff_datetime,
    "PULocationID" AS pulocationid,
    "DOLocationID" AS dolocationid,
    "SR_Flag" AS sr_flag
FROM
    {{ source(
        'staging',
        'fhv_tripdata_' + date
    ) }}

    {% if loop.index <= 11 %}
    UNION ALL
    {% endif %}
{% endfor %}

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

