from ethereumetl_airflow.build_export_dag import build_export_dag
from ethereumetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_export_dag(
    dag_id='ethereum_export_dag',
    **read_export_dag_vars(
        var_prefix='ethereum_',
        provider_uris = 'https://base-rpc.publicnode.com',
        export_schedule_interval='0 8 * * *',
        export_start_date='2015-07-30',
        export_max_workers=10,
        export_batch_size=10,
        export_retries=5,
    )
)