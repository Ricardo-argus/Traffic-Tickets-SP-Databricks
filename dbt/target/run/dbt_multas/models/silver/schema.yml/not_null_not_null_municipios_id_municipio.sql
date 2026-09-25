
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_municipio
from `workspace`.`multas_analytics`.`not_null_municipios`
where id_municipio is null



  
  
      
    ) dbt_internal_test