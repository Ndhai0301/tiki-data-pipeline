
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour
from "tiki"."staging"."stg_price_readings"
where hour is null



  
  
      
    ) dbt_internal_test