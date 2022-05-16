from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from voucher_selector.utils.prepare_data import prepare_vouchers_region

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
        default_args=default_args,
        dag_id='voucher_selector_dag',
        schedule_interval="@daily",
        catchup=False,
        max_active_runs=1,
) as dag:

    # task to load given customer segments
    load_pg_customer_segments = PostgresOperator(
        task_id='load_pg_customer_segments',
        postgres_conn_id='postgres_default',
        sql='sql/customer_segments.sql'
    )

    # task to process vouchers for Peru customers
    prepare_vouchers_peru = PythonOperator(
        task_id='prepare_vouchers_peru',
        dag=dag,
        python_callable=prepare_vouchers_region,
        op_kwargs={
            # "input_s3": "s3://dh-data-chef-hiring-test/data-eng/voucher-selector/data.parquet.gzip",
            "input_s3": "https://dh-data-chef-hiring-test.s3.eu-central-1.amazonaws.com/data-eng/voucher-selector/data.parquet.gzip",
            "schema": "voucher_customer",
            "input_tbl": "customer_segments",
            "output_tbl1": "voucher_segments",
            "output_tbl2": "voucher_selector",
        },
    )

load_pg_customer_segments >> prepare_vouchers_peru
