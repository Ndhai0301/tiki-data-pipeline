
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ngay
from "tiki"."gold"."dim_date"
where ngay is null



  
  
      
    ) dbt_internal_test