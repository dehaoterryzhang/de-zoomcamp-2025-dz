services:
	postgres:
		image: postgres:13
		environment:
			POSTGRES_USER: airflow
			POSTGRES_PASSWORD: airflow
			POSTGRES_DB: airflow
		volumes:
			- postgres-db-volume:/var/lib/postgresql/data
		healthcheck:
			test: ["CMD", "pg_isready", "-U", "airflow"]
			interval: 5s
			retries: 5
		restart: always

docker volume create --name dtc_postgres_volume_local -d local

docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v dtc_postgres_volume_local:/var/lib/postgresql/data \
	-p 5432:5432 \
	postgres:13

docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v //c/Users/dehzhang/Desktop/GitHub-Repos/de-zoomcamp/01-docker-terraform/notes-practice/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 \
	postgres:13

pgcli \
	-h  localhost -p 5432 \
	-u root \
	-d ny_taxi

docker run -it \
	-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4


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

docker run -it \
	-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
	--network pg-network \
	--name pgadmin \
  dpage/pgadmin4

URL="<https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz>"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="${URL}"


docker build -t taxi_ingest:v001 .

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url="${URL}"