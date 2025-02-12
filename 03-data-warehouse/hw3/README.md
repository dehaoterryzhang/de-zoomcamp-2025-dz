# Q1

After the parquet files are loaded to GCS and we create a table in BigQuery, 

```sql
select count(*) from nytaxi.yellow_tripdata_non_partitoned
```

The result returned 20,332,093 rows.

# Q2

For External Table:

```sql
select distinct PULocationID from nytaxi.external_yellow_tripdata
```

When highlighting this query, the estimation shows "will process 0 B when run"

For Materialized Table:

```sql
select distinct PULocationID from nytaxi.yellow_tripdata_non_partitoned
```

When highlighting this query, the estimation shows "will process 155.12 MB when run"


# Q3

The two queries:

```sql
select PULocationID from nytaxi.yellow_tripdata_non_partitoned
select PULocationID, DOLocationID from nytaxi.yellow_tripdata_non_partitoned
```

The estimated bytes for the second one is twice as much as the first one since Big Query is columnar and store columns separately.

# Q4

```sql
select count(*) from nytaxi.yellow_tripdata_non_partitoned where fare_amount = 0
```

The output was 8,333 rows

# Q5

Since it filters based on tpep_dropoff_datetime, it should be partition by it.
Since it needs to order by results by VendorID, it should be cluster by it.

# Q6

Queries:

```sql
CREATE OR REPLACE TABLE nytaxi.yellow_tripdata_partitoned_tpep_dropoff_datetime_clustered_vendorid
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM nytaxi.external_yellow_tripdata;

select distinct VendorID from nytaxi.yellow_tripdata_non_partitoned
where tpep_dropoff_datetime >= "2024-03-01" and tpep_dropoff_datetime <= "2024-03-15"

select distinct VendorID from nytaxi.yellow_tripdata_partitoned_tpep_dropoff_datetime_clustered_vendorid
where tpep_dropoff_datetime >= "2024-03-01" and tpep_dropoff_datetime <= "2024-03-15"
```

In the top select statement, the estimate shows "310.24 MB" and the second one shows "26.86 MB"

# Q7

GCP Bucket

# Q8

Clustering data is not always necessary. For example, when the data is small, it doesn't show signficant improvement results.

# Q9

It estimates 0 B processed, it's because BigQuery is using the precomputed metadata stored in the materialized table instead of scanning the entire dataset.