
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour
from "tiki"."gold"."fact_price_daily"
where hour is null



  
  
      
    ) dbt_internal_test