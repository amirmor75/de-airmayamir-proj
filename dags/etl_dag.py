from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fetch the Trino connection ID from Airflow Variables
trino_conn_id = Variable.get("TRINO_CONN_ID")  # <-- Defined in Airflow UI

with DAG(
    dag_id='airmayamir',
    default_args=default_args,
    description='airmayamir',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['trino', 'iceberg', 'hive', 'dedup', 'hash']
) as dag:

    insert_new_payment_methods = TrinoOperator(
        task_id='insert_new_payment_methods',
        sql="""
            INSERT INTO iceberg.analytics.payment_methods_summary (
                method_name,
                code
            )
            SELECT
                pm.payment_method AS method_name,
                xxhash64(pm.payment_method) AS code
            FROM (
                SELECT DISTINCT payment_method
                FROM hive.payme.payments
                WHERE payment_date >= DATE '{{ ds }}'
                  AND payment_date < DATE '{{ next_ds }}'
            ) pm
            WHERE NOT EXISTS (
                SELECT 1
                FROM iceberg.analytics.payment_methods_summary existing
                WHERE existing.method_name = pm.payment_method
            );
        """,
        trino_conn_id=trino_conn_id  # <-- Dynamically set from Variable
    )
