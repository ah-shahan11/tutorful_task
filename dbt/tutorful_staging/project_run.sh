echo "Running dbt project..."
dbt deps
dbt run --select bookings
echo "DBT project run complete."