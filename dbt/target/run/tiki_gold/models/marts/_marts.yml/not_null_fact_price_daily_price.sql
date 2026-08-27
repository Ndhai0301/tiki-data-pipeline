
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price
from "tiki"."gold"."fact_price_daily"
where price is null



  
  
      
    ) dbt_internal_test