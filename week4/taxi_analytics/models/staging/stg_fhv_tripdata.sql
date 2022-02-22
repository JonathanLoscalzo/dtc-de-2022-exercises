{{ config(
  materialized = 'table'
) }}

WITH stg_fhv_tripdata AS (

  SELECT
    *
  FROM
    {{ ref('fhv_tripdata_2019') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('fhv_tripdata_2020') }}
)
SELECT
  -- identifiers
  {{ dbt_utils.surrogate_key(['index', 'pickup_datetime', 'pulocationid']) }} AS pk_fact_fhv_trips,
  CAST(
    index AS INTEGER
  ) AS index,
  CAST(
    dispatching_base_num AS VARCHAR
  ) AS dispatching_base_num,
  CAST(
    pickup_datetime AS timestamp
  ) AS pickup_datetime,
  CAST(
    dropoff_datetime AS timestamp
  ) AS dropoff_datetime,
  CAST(
    pulocationid AS INTEGER
  ) AS pulocationid,
  CAST(
    dolocationid AS INTEGER
  ) AS dolocationid,
  CAST(
    sr_flag AS INTEGER
  ) AS sr_flag
FROM
  stg_fhv_tripdata 
  {% if var(
      'is_test_run',
      default = true
    ) %}
  LIMIT
    100
  {% endif %}
