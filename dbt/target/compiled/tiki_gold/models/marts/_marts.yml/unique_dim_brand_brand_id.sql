
    
    

select
    brand_id as unique_field,
    count(*) as n_records

from "tiki"."gold"."dim_brand"
where brand_id is not null
group by brand_id
having count(*) > 1


