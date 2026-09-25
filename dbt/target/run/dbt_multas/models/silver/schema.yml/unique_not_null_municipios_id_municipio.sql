
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    id_municipio as unique_field,
    count(*) as n_records

from `workspace`.`multas_analytics`.`not_null_municipios`
where id_municipio is not null
group by id_municipio
having count(*) > 1



  
  
      
    ) dbt_internal_test