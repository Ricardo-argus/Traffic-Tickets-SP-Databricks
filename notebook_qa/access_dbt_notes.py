pip install dbt-databricks


%sh cd /Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks


%sh dbt run --profiles-dir /Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/dbt \
            --project-dir /Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/dbt


%sh dbt test --profiles-dir /Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/dbt \
            --project-dir /Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/dbt

