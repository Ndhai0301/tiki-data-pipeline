
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dt
from "tiki"."gold"."fact_price_daily"
where dt is null



  
  
      
    ) dbt_internal_test