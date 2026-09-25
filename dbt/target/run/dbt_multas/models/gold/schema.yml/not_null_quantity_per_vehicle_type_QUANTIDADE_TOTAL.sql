
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select QUANTIDADE_TOTAL
from `workspace`.`multas_analytics`.`quantity_per_vehicle_type`
where QUANTIDADE_TOTAL is null



  
  
      
    ) dbt_internal_test