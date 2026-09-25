
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    qtd_municipios_validos as unique_field,
    count(*) as n_records

from `workspace`.`multas_analytics`.`quantity_municipios`
where qtd_municipios_validos is not null
group by qtd_municipios_validos
having count(*) > 1



  
  
      
    ) dbt_internal_test