echo "Running dbt project..."
dbt deps
dbt run 
echo "DBT project run complete."