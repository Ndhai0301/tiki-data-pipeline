
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dt
from "tiki"."staging"."stg_price_readings"
where dt is null



  
  
      
    ) dbt_internal_test