{{ config(
    schema=resolve_schema_for('non-core'), 
) }}

select * from {{ ref('taxi_zone_lookup') }}