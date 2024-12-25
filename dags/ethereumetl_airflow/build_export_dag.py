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

def get_project_directory() -> str:
    return os.path.abspath(os.path.join(__file__, "../../.."))
project_dir = get_project_directory()

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
        tags=None,
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
        max_active_runs=export_max_active_runs,
        tags=tags,
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

        # Check if bucket exists and create if not
        if not storage_hook.check_for_bucket(output_bucket):
            logging.info(f"Bucket {output_bucket} does not exist. Creating bucket.")
            storage_hook.create_bucket(bucket_name=output_bucket)

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
        
        return int(start_block), int(end_block)

    def export_blocks_and_transactions_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)

            logging.info('Calling export_blocks_and_transactions({}, {}, {}, {}, {}, ...)'.format(
                start_block, end_block, export_batch_size, provider_uri, export_max_workers))

            export_blocks_and_transactions.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                blocks_output=os.path.join(tempdir, "blocks.csv"),
                transactions_output=os.path.join(tempdir, "transactions.csv"),
            )

            copy_to_export_path(
                os.path.join(tempdir, "blocks_meta.txt"), export_path("blocks_meta", logical_date)
            )

            copy_to_export_path(
                os.path.join(tempdir, "blocks.csv"), export_path("blocks", logical_date)
            )

            copy_to_export_path(
                os.path.join(tempdir, "transactions.csv"), export_path("transactions", logical_date)
            )
        
    def export_receipts_and_logs_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            copy_from_export_path(
                export_path("transactions", logical_date), os.path.join(tempdir, "transactions.csv")
            )

            logging.info('Calling extract_csv_column(...)')
            extract_field.callback(
                input=os.path.join(tempdir, "transactions.csv"),
                output=os.path.join(tempdir, "transaction_hashes.txt"),
                field="hash",
            )

            logging.info('Calling export_receipts_and_logs({}, ..., {}, {}, ...)'.format(
                export_batch_size, provider_uri, export_max_workers))
            export_receipts_and_logs.callback(
                batch_size=export_batch_size,
                transaction_hashes=os.path.join(tempdir, "transaction_hashes.txt"),
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                receipts_output=os.path.join(tempdir, "receipts.csv"),
                logs_output=os.path.join(tempdir, "logs.csv"),
            )

            copy_to_export_path(
                os.path.join(tempdir, "receipts.csv"), export_path("receipts", logical_date)
            )
            copy_to_export_path(os.path.join(tempdir, "logs.csv"), export_path("logs", logical_date))

    def extract_contracts_command(logical_date, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            copy_from_export_path(
                export_path("traces", logical_date), os.path.join(tempdir, "traces.csv")
            )

            logging.info('Calling extract_contracts(..., {}, {})'.format(
                export_batch_size, export_max_workers
            ))
            extract_contracts.callback(
                traces=os.path.join(tempdir, "traces.csv"),
                output=os.path.join(tempdir, "contracts.csv"),
                batch_size=export_batch_size,
                max_workers=export_max_workers,
            )

            copy_to_export_path(
                os.path.join(tempdir, "contracts.csv"), export_path("contracts", logical_date)
            )

    def extract_tokens_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            copy_from_export_path(
                export_path("contracts", logical_date), os.path.join(tempdir, "contracts.csv")
            )

            logging.info('Calling extract_tokens(..., {}, {})'.format(export_max_workers, provider_uri))
            extract_tokens.callback(
                contracts=os.path.join(tempdir, "contracts.csv"),
                output=os.path.join(tempdir, "tokens.csv"),
                max_workers=export_max_workers,
                provider_uri=provider_uri,
                values_as_strings=True,
            )

            copy_to_export_path(
                os.path.join(tempdir, "tokens.csv"), export_path("tokens", logical_date)
            )

    def extract_token_transfers_command(logical_date, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            copy_from_export_path(
                export_path("logs", logical_date), os.path.join(tempdir, "logs.csv")
            )

            logging.info('Calling extract_token_transfers(..., {}, ..., {})'.format(
                export_batch_size, export_max_workers
            ))
            extract_token_transfers.callback(
                logs=os.path.join(tempdir, "logs.csv"),
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "token_transfers.csv"),
                max_workers=export_max_workers,
                values_as_strings=True,
            )

            copy_to_export_path(
                os.path.join(tempdir, "token_transfers.csv"),
                export_path("token_transfers", logical_date),
            )

    def export_traces_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)

            logging.info('Calling export_traces({}, {}, {}, ...,{}, {}, {}, {})'.format(
                start_block, end_block, export_batch_size, export_max_workers, provider_uri,
                export_genesis_traces_option, export_daofork_traces_option
            ))
            export_traces.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "traces.csv"),
                max_workers=export_max_workers,
                provider_uri=provider_uri,
                genesis_traces=export_genesis_traces_option,
                daofork_traces=export_daofork_traces_option,
            )

            copy_to_export_path(
                os.path.join(tempdir, "traces.csv"), export_path("traces", logical_date)
            )

    def add_export_task(toggle, task_id, python_callable, dependencies=None):
        if toggle:
            operator = PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                execution_timeout=timedelta(hours=15),
                dag=dag,
            )
            if dependencies is not None and len(dependencies) > 0:
                for dependency in dependencies:
                    if dependency is not None:
                        dependency >> operator
            return operator
        else:
            return None
    
    # Operators
    def add_provider_uri_fallback_loop(python_callable, provider_uris):
        """Tries each provider uri in provider_uris until the command succeeds"""

        def python_callable_with_fallback(**kwargs):
            for index, provider_uri in enumerate(provider_uris):
                kwargs['provider_uri'] = provider_uri
                try:
                    python_callable(**kwargs)
                    break
                except Exception as e:
                    if index < (len(provider_uris) - 1):
                        logging.exception('An exception occurred. Trying another uri')
                    else:
                        raise e

        return python_callable_with_fallback

    export_blocks_and_transactions_operator = add_export_task(
        export_blocks_and_transactions_toggle,
        "export_blocks_and_transactions",
        add_provider_uri_fallback_loop(export_blocks_and_transactions_command, provider_uris),
    )

    export_receipts_and_logs_operator = add_export_task(
        export_receipts_and_logs_toggle,
        "export_receipts_and_logs",
        add_provider_uri_fallback_loop(export_receipts_and_logs_command, provider_uris),
        dependencies=[export_blocks_and_transactions_operator],
    )

    extract_token_transfers_operator = add_export_task(
        extract_token_transfers_toggle,
        "extract_token_transfers",
        extract_token_transfers_command,
        dependencies=[export_receipts_and_logs_operator],
    )

    export_traces_operator = add_export_task(
        export_traces_toggle,
        "export_traces",
        add_provider_uri_fallback_loop(export_traces_command, provider_uris_archival)
    )

    extract_contracts_operator = add_export_task(
        extract_contracts_toggle,
        "extract_contracts",
        extract_contracts_command,
        dependencies=[export_traces_operator],
    )

    extract_tokens_operator = add_export_task(
        extract_tokens_toggle,
        "extract_tokens",
        add_provider_uri_fallback_loop(extract_tokens_command, provider_uris),
        dependencies=[extract_contracts_operator],
    )

    return dag


    