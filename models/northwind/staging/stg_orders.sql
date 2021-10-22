with pedidos as 
    source as (
      order_id 
        , employee_id 
        , order_date 
        , customer_id 
        , ship_region 
        , shipped_date 
        , ship_country 
        , ship_name 
        , ship_postal_code 
        , ship_city 
        , freight 
        , ship_via 
        , ship_address 
        , required_date 
    from {{ source('erpNorthwind16102021', 'orders')}}
    )
select * from source