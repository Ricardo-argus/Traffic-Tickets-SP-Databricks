
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TIPO_VEICULO
from `workspace`.`multas_analytics`.`quantity_per_vehicle_type`
where TIPO_VEICULO is null



  
  
      
    ) dbt_internal_test