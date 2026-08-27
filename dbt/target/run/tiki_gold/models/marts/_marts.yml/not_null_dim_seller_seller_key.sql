
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seller_key
from "tiki"."gold"."dim_seller"
where seller_key is null



  
  
      
    ) dbt_internal_test