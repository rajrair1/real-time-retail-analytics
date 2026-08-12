from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    "retail_quality_checks",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["portfolio", "data-quality"],
) as dag:
    assert_fresh_data = PostgresOperator(
        task_id="assert_fresh_data",
        postgres_conn_id="retail_postgres",
        sql="SELECT CASE WHEN max(event_time) > now() - interval '20 minutes' THEN 1 ELSE 1/0 END FROM fact_sales;",
    )
    assert_no_invalid_amounts = PostgresOperator(
        task_id="assert_no_invalid_amounts",
        postgres_conn_id="retail_postgres",
        sql="SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 1/0 END FROM fact_sales WHERE net_amount < 0;",
    )
    assert_fresh_data >> assert_no_invalid_amounts
