from airflow import DAG
from datetime import datetime
from cosmos.providers.dbt.core.dag import DbtDag
from cosmos.config import (
    DbtProjectConfig,
    ExecutionConfig,
    ProfileConfig,
    ProfileMapping,
)
from cosmos.constants import LoadMode
from pathlib import Path

# Path to your dbt project
DBT_PROJECT_PATH = Path("../dbt/AirMayAmir")

# Profile config for Trino + Iceberg
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=ProfileMapping(
        profile_args={
            "type": "trino",
            "threads": 1,
            "host": "trino.de.bsmch.net",
            "port": 8080,
            "user": "hiveuser",
            "catalog": "iceberg",
            "schema": "dbt_schema",
            "http_scheme": "http"
        }
    )
)

# DAG setup
with DAG(
    dag_id="etl_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "cosmos", "per-model"],
) as dag:

    dbt_dag = DbtDag(
        project_config=DbtProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            project_name="AirMayAmir"
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt"
        ),
        load_mode=LoadMode.DBT_LS,  # Uses `dbt ls` to discover models
        operator_args={
            "install_deps": False  # skip deps install in prod
        },
    )

    dbt_dag  # registers the per-model tasks
