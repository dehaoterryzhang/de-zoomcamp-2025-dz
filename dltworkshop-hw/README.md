# Q1

```bash
!pip install dlt[duckdb]

!dlt --version
```

The output was dlt 1.6.1.

# Q2

The added code was 

```python
@dlt.resource(name="rides")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # <--- yield data to manage memory
```

After connecting to the database, the desribe function shows 4 tables.

# Q3

```python
len(df)
```

The result was 10000.

# Q4

Running the provided code, it gave 12.3049.

