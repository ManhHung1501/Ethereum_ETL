import os
import logging
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow import DAG, configuration
from airflow.operators.python import PythonOperator

from ethereumetl.cli import (
    get_block_range_for_date,
    export_blocks_and_transactions,
    export_receipts_and_logs,
    extract_contracts,
    extract_tokens,
    extract_token_transfers,
    export_traces,
    extract_field,
)

from common import project_dir

DATA_DIR = f"{project_dir}/data/"
TEMP_DIR = DATA_DIR if Path(DATA_DIR).exists() else None

def build_export_dag(
        dag_id,
        provider_uris,
        provider_uris_archival,
        output_bucket,
        export_start_date,
        export_end_date=None,
        notification_emails=None,
        export_schedule_interval='0 0 * * *',
        export_max_workers=10,
        export_batch_size=10,
        export_max_active_runs=None,
        export_retries=5,
        **kwargs
):
    default_dag_args = {
        "depends_on_past": False,
        "start_date": export_start_date,
        "end_date": export_end_date,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": export_retries,
        "retry_delay": timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    export_daofork_traces_option = kwargs.get('export_daofork_traces_option')
    export_genesis_traces_option = kwargs.get('export_genesis_traces_option')
    export_blocks_and_transactions_toggle = kwargs.get('export_blocks_and_transactions_toggle')
    export_receipts_and_logs_toggle = kwargs.get('export_receipts_and_logs_toggle')
    extract_contracts_toggle = kwargs.get('extract_contracts_toggle')
    extract_tokens_toggle = kwargs.get('extract_tokens_toggle')
    extract_token_transfers_toggle = kwargs.get('extract_token_transfers_toggle')
    export_traces_toggle = kwargs.get('export_traces_toggle')

    if export_max_active_runs is None:
        export_max_active_runs = configuration.conf.getint('core', 'max_active_runs_per_dag')

    dag = DAG(
        dag_id,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
        max_active_runs=export_max_active_runs
    )


    from airflow.hooks.S3_hook import S3Hook
    storage_hook = S3Hook(aws_conn_id="minio_s3_conn")


    # Export
    def export_path(directory, date):
        return "export/{directory}/block_date={block_date}/".format(
            directory=directory, block_date=date.strftime("%Y-%m-%d")
        )

    def copy_to_export_path(file_path, export_path):
        logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
        filename = os.path.basename(file_path)

        storage_hook.load_file(
            filename=file_path,
            bucket_name=output_bucket,
            key=export_path + filename,
            replace=True,
            encrypt=False
        )

    def copy_from_export_path(export_path, file_path):
        logging.info('Calling copy_from_export_path({}, {})'.format(export_path, file_path))
        filename = os.path.basename(file_path)
        # boto3.s3.Object
        s3_object = storage_hook.get_key(
            bucket_name=output_bucket,
            key=export_path + filename
        )
        s3_object.download_file(file_path)
    
    def get_block_range(tempdir, date, provider_uri):
        logging.info('Calling get_block_range_for_date({}, {}, ...)'.format(provider_uri, date))
        get_block_range_for_date.callback(
            provider_uri=provider_uri, date=date, output=os.path.join(tempdir, "blocks_meta.txt")
        )

        with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
            block_range = block_range_file.read()
            start_block, end_block = block_range.split(",")