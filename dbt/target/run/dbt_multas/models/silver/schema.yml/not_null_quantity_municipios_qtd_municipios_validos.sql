
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select qtd_municipios_validos
from `workspace`.`multas_analytics`.`quantity_municipios`
where qtd_municipios_validos is null



  
  
      
    ) dbt_internal_test