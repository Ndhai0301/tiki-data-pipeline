
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select brand_key
from "tiki"."gold"."dim_brand"
where brand_key is null



  
  
      
    ) dbt_internal_test