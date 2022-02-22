{% set dates_2019 = [ "2019_01", "2019_02", "2019_03", "2019_04", "2019_05", "2019_06", "2019_07", "2019_08", "2019_09", "2019_10", "2019_11", "2019_12", ] %}
{% for date in dates_2019 %}
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

