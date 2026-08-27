
    
    



select dbt_valid_from
from "tiki"."gold"."dim_product_snapshot"
where dbt_valid_from is null


