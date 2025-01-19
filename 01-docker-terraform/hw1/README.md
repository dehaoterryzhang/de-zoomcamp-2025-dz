# Q1

shell command: 
```bash
docker run -it --entrypoint=bash python:3.12.8
```

Then
```bash
pip --version
```

The output was "pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)"


# Q2

First, copy the content into docker-compose.yaml. Then,

shell command:
```bash
docker-compose.yaml
```

After specifying 'db' as host name and '5432' as the port, it successfully connected.


# Q3

First, start a postgresql container:

```bash
docker network create pg-network

docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v //c/Users/dehzhang/Desktop/GitHub-Repos/de-zoomcamp/01-docker-terraform/notes-practice/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 \
	--network pg-network \
	--name pg-database \
	postgres:13
```

Then, run another container to ingest the green taxi data

```bash
docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=green_taxi_trips \
  --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
```

Then run load_zone.ipynb to load the zone_lookup csv file too.

Finally, run another container for pgadmin

```bash
docker run -it \
	-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
	--network pg-network \
	--name pgadmin \
  dpage/pgadmin4
```

SQL command:
```sql
select sum(case when trip_distance <= 1 then 1 else 0 end) as Upto1Mile,
sum(case when trip_distance > 1 and trip_distance <=3 then 1 else 0 end) as Morethan1Upto3Mile,
sum(case when trip_distance > 3 and trip_distance <=7 then 1 else 0 end) as Morethan3Upto7Mile,
sum(case when trip_distance > 7 and trip_distance <=10 then 1 else 0 end) as Morethan7Upto10Mile,
sum(case when trip_distance > 10 then 1 else 0 end) as Over10Mile
from green_taxi_trips where lpep_pickup_datetime >= '2019-10-01' and lpep_dropoff_datetime < '2019-11-01'
```

The output was 104,802; 198,924; 109,603; 27,678; 35,189.


# Q4

SQL command:
```sql
SELECT DATE(lpep_pickup_datetime) from green_taxi_trips WHERE trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_trips);
```

The output was 2019-10-31.

# Q5

SQL command:
```sql
select SUM(t.total_amount), z1."Zone" as pickupzone from green_taxi_trips t
LEFT JOIN zone_lookup z1 on t."PULocationID" = z1."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY z1."Zone"
HAVING SUM(t.total_amount) > 13000
ORDER BY SUM(t.total_amount) desc
```

The 3 pick up zones are East Harlem North, East Harlem South, Morningside Heights.


# Q6

```sql
with filtered as (
select lpep_pickup_datetime, z1."Zone" as pickupzone, z2."Zone" as dropoffzone, tip_amount from green_taxi_trips t
LEFT JOIN zone_lookup z1 on t."PULocationID" = z1."LocationID"
LEFT JOIN zone_lookup z2 on t."DOLocationID" = z2."LocationID"
WHERE z1."Zone" = 'East Harlem North' and DATE(lpep_pickup_datetime) >= '2019-10-01' and 
DATE(lpep_pickup_datetime) < '2019-11-01')
select dropoffzone from filtered order by tip_amount desc
LIMIT 1
```

The output is JFK Airport.


# Q7

In GCP VM, first

```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/my-creds.json
```

Then using main.tf, 

```bash
terraform init
terraform apply -auto-approve
terraform destory
```