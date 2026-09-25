
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    TIPO_VEICULO as unique_field,
    count(*) as n_records

from `workspace`.`multas_analytics`.`quantity_per_vehicle_type`
where TIPO_VEICULO is not null
group by TIPO_VEICULO
having count(*) > 1



  
  
      
    ) dbt_internal_test