
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select changed_at
from "tiki"."gold"."fact_price_change"
where changed_at is null



  
  
      
    ) dbt_internal_test