{{ config(materialized='table') }}
with
    customer as (
        select
            customer_sk
            customers_id
        from {{ref('dim_customers')}}
    )

,  orders_with_sk as (
        select
            orders.order_id
          , customers.customer_sk as customer_fk
          , employees.employee_sk as employee_fk        
          , orders.order_date                  
          , orders.ship_region
          , orders.shipped_date
          , orders.ship_country
          , orders.ship_address
          , orders.ship_postal_code
          , orders.ship_city
          , shippers.shipper_id as shipper_fk
          , orders.ship_name
          , orders.freight
          , orders.required_date

        from {{ ref('stg_orders') }} orders
        left join customers on orders.customer_id = custumers.customer_id
)        
select * from orders_with_sk