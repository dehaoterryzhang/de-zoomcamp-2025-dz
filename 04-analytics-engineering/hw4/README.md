# Q1

After setting up sources.yml and the environment variables, compile the .sql model outputs

```bash
select * 
from `myproject`.`raw_nyc_tripdata`.`ext_green_taxi`
```

# Q2

For command line arguments to take precedence over ENV_VARs, we need var on the outside and then env_var on the inside.
So, it would be CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY.

# Q3

dbt run --select models/staging/+ would not materialize it because dim_zones.sql would not have been run.

# Q4

The first statement is true because without the value of that env variable it would not compile.
The second statement is not true because without the vaule of that env variable, if doesn't affect when model_type == 'core' and when model_type is not 'core' it can use target_env_var if stging_env_var is not set.
The third to fifth statements are true because they follow the macro logic.


# Q5

code for the .sql file:

```sql
{{
    config(
        materialized='table'
    )
}}

with temp as (
    SELECT EXTRACT(YEAR FROM pickup_datetime) AS year, 
EXTRACT(QUARTER FROM pickup_datetime) AS quarter, service_type, total_amount
    from {{ ref('fact_trips') }}
),
grouped as (
    select service_type, year, quarter, sum(total_amount) as total_amount from temp
group by service_type, year, quarter
)
SELECT 
    service_type,
    year,
    quarter,
    total_amount,
    LAG(total_amount) OVER (
        PARTITION BY service_type, quarter ORDER BY year
    ) AS prev_year_total_amount,
    CASE 
        WHEN LAG(total_amount) OVER (
            PARTITION BY service_type, quarter ORDER BY year
        ) = 0 THEN NULL  -- Avoid division by zero
        ELSE ROUND(
            (total_amount - LAG(total_amount) OVER (
                PARTITION BY service_type, quarter ORDER BY year
            )) / NULLIF(LAG(total_amount) OVER (
                PARTITION BY service_type, quarter ORDER BY year
            ), 0) * 100, 2
        )
    END AS yoy_percentage_change
FROM grouped
ORDER BY service_type, year, quarter
```

For green, the best YoY was -56.41% in 2020Q1 and worst was -92.82% in 2020Q2.
For yellow, the best YoY was -19.93% in 2020Q1 and worst was -92.57% in 2020Q2.

# Q6

code for the .sql file:

```sql
{{
    config(
        materialized='view'
    )
}}

with temp as (
    SELECT EXTRACT(YEAR FROM pickup_datetime) AS year, 
    EXTRACT(MONTH FROM pickup_datetime) AS month, 
    service_type, fare_amount FROM
    {{ ref('fact_trips') }}
    where fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit card')
)
select service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97
    from temp where year = 2020 and month = 4
    order by service_type, year, month
```

Numbers in the order of p97, p95, p90:
Green: 55.0, 45.0, 26.5
Yellow: 32.0, 26.0, 19.0


# Q7

staging.sql:
```sql
{{ config(materialized='view') }}

with tripdata as 
(
  select *,
  from {{ source('staging','fhv_tripdata') }}
  where Dispatching_base_num is not null 
)
select
    unique_row_id,
    Dispatching_base_num,
    cast(Pickup_datetime as timestamp) as pickup_datetime,
    cast(DropOff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("PULocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOLocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    SR_Flag
from tripdata
```

.sql file for dim:
```sql
{{
    config(
        materialized='table'
    )
}}

with tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    tripdata.unique_row_id,
    tripdata.Dispatching_base_num,
    tripdata.pickup_datetime,
    tripdata.dropoff_datetime,
    tripdata.pickup_locationid,
    tripdata.dropoff_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    tripdata.SR_Flag,
    EXTRACT(YEAR FROM pickup_datetime) AS year, 
    EXTRACT(MONTH FROM pickup_datetime) AS month
from tripdata
inner join dim_zones as pickup_zone
on tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on tripdata.dropoff_locationid = dropoff_zone.locationid
```

.sql for fact table:
```sql
{{
    config(
        materialized='view'
    )
}}

with temp as (
    Select unique_row_id,
    Dispatching_base_num,
    pickup_locationid,
    dropoff_locationid,
    pickup_datetime,
    dropoff_datetime,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
    pickup_zone,
    dropoff_zone,
    year,
    month
from {{ ref('dim_fhv_trips') }}
where pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East') and year = 2019 and month = 11
)
select 
    year, 
    month,
    pickup_locationid,
    dropoff_locationid,
    pickup_zone,
    dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) as p90
from temp
order by
    pickup_zone, p90 desc
``` 

The second p90 dropoff zones respectively are:
LaGuardia Airport, Chinatown, Garment District
