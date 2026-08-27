
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price_moi
from "tiki"."gold"."fact_price_change"
where price_moi is null



  
  
      
    ) dbt_internal_test