with
    source_data as (
        select
            customer_id		
            , contact_name	
            , contact_title	
            , address	
            , city	
            , region	
            , postal_code	
            , country	
            , phone	
            , fax
            company_name
        from {{ source ('erpNorthwind16102021','customers')}}
        )