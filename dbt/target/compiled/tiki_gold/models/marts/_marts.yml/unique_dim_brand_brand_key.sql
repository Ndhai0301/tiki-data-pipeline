
    
    

select
    brand_key as unique_field,
    count(*) as n_records

from "tiki"."gold"."dim_brand"
where brand_key is not null
group by brand_key
having count(*) > 1


