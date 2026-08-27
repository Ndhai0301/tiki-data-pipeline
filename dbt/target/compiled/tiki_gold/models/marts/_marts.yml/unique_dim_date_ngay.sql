
    
    

select
    ngay as unique_field,
    count(*) as n_records

from "tiki"."gold"."dim_date"
where ngay is not null
group by ngay
having count(*) > 1


