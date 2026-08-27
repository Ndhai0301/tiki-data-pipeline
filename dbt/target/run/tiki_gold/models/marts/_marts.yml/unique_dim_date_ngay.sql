
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    ngay as unique_field,
    count(*) as n_records

from "tiki"."gold"."dim_date"
where ngay is not null
group by ngay
having count(*) > 1



  
  
      
    ) dbt_internal_test