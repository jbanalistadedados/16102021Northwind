{{ config(materialized='table') }}
with
    source as 
        from {{source('erpNorthwind16102021','public_products')}}
    )

select *
from source_data