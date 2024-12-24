import os
from datetime import datetime

from airflow.models import Variable

def get_project_directory() -> str:
    return os.path.abspath(os.path.join(__file__, "../../.."))
project_dir = get_project_directory()

def read_var(var_name, var_prefix=None, required=False, **kwargs):
    full_var_name = f'{var_prefix}{var_name}' if var_prefix is not None else var_name
    var = Variable.get(full_var_name, '')
    var = var if var != '' else None
    if var is None:
        var = kwargs.get(var_name)
    if required and var is None:
        raise ValueError(f'{full_var_name} variable is required')
    return var

def parse_bool(bool_string, default=True):
    if isinstance(bool_string, bool):
        return bool_string
    if bool_string is None or len(bool_string) == 0:
        return default
    else:
        return bool_string.lower() in ["true", "yes"]

def parse_int(val):
    if val is None:
        return None
    return int(val)

def read_export_dag_vars(var_prefix, **kwargs):
    export_start_date = read_var('export_start_date', var_prefix, True, **kwargs)
    export_start_date = datetime.strptime(export_start_date, '%Y-%m-%d')
    
    export_end_date = read_var('export_end_date', var_prefix, False, **kwargs)
    export_end_date = datetime.strptime(export_end_date, '%Y-%m-%d') if export_end_date is not None else None
    
    provider_uris = read_var('provider_uris', var_prefix, True, **kwargs)
    provider_uris = [uri.strip() for uri in provider_uris.split(',')]

    provider_uris_archival = read_var('provider_uris_archival', var_prefix, False, **kwargs)
    if provider_uris_archival is None:
        provider_uris_archival = provider_uris
    else:
        provider_uris_archival = [uri.strip() for uri in provider_uris_archival.split(',')]


    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'export_start_date': export_start_date,
        'export_end_date': export_end_date,
        'export_schedule_interval': read_var('export_schedule_interval', var_prefix, True, **kwargs),
        'provider_uris': provider_uris,
        'provider_uris_archival': provider_uris_archival,
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'export_max_active_runs': parse_int(read_var('export_max_active_runs', var_prefix, False, **kwargs)),
        'export_max_workers': parse_int(read_var('export_max_workers', var_prefix, True, **kwargs)),
        'export_batch_size': parse_int(read_var('export_batch_size', var_prefix, True, **kwargs)),
        'export_retries': parse_int(read_var('export_retries', var_prefix, True, **kwargs)),
        # toggles
        'export_daofork_traces_option': parse_bool(
            read_var('export_daofork_traces_option', var_prefix, False, **kwargs)),
        'export_genesis_traces_option': parse_bool(
            read_var('export_genesis_traces_option', var_prefix, False, **kwargs)),
        'export_blocks_and_transactions_toggle': parse_bool(
            read_var('export_blocks_and_transactions_toggle', var_prefix, False, **kwargs)),
        'export_receipts_and_logs_toggle': parse_bool(
            read_var('export_receipts_and_logs_toggle', var_prefix, False, **kwargs)),
        'extract_contracts_toggle': parse_bool(
            read_var('extract_contracts_toggle', var_prefix, False, **kwargs)),
        'extract_tokens_toggle': parse_bool(
            read_var('extract_tokens_toggle', var_prefix, False, **kwargs)),
        'extract_token_transfers_toggle': parse_bool(
            read_var('extract_token_transfers_toggle', var_prefix, False, **kwargs)),
        'export_traces_toggle': parse_bool(
            read_var('export_traces_toggle', var_prefix, False, **kwargs)),
    }

    return vars