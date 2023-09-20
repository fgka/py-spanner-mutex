# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Wrapper around Cloud Spanner Python API, which is a thin layer on top of `Cloud Spanner API`_.

.. _Cloud Spanner API: https://cloud.google.com/python/docs/reference/spanner/latest
"""
import os
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

from google import auth
from google.auth import credentials
from google.cloud import spanner
from google.cloud.spanner_v1 import database, instance, streamed, table, transaction

from py_spanner_mutex.common import logger, preprocess

_SPANNER_EMULATOR_PROJECT_NAME: str = "spanner_emulator"
_DEFAULT_SPANNER_EMULATOR_HOST: str = "0.0.0.0:9010"
_SPANNER_EMULATOR_HOST: str = os.environ.get("SPANNER_EMULATOR_HOST", _DEFAULT_SPANNER_EMULATOR_HOST)
_SPANNER_EMULATOR_CLIENT_OPTIONS: Dict[str, str] = {"api_endpoint": _SPANNER_EMULATOR_HOST}
SPANNER_USE_EMULATOR_ENV_VAR: str = "SPANNER_USE_EMULATOR"
SPANNER_USE_EMULATOR_ENV_VAR_VALUE: str = "YES"
"""
Set this environment variable to any value that is not empty and it will enable the Spanner emulator.
"""

_LOGGER = logger.get(__name__)


class SpannerError(Exception):
    """
    Wraps all lower level exceptions from Cloud Spanner.
    """


def spanner_table(*, db: database.Database, table_id: str, must_exist: Optional[bool] = True) -> table.Table:
    """
    Wrapper around API with error checking.
    Args:
        db:
        table_id:
        must_exist:

    Returns:

    """
    _LOGGER.debug("Getting '%s' using '%s'", table.Table.__name__, locals())
    # input validation
    preprocess.validate_type(db, "db", database.Database)
    preprocess.string(table_id, "table_id")
    preprocess.validate_type(must_exist, "must_exist", bool)
    # logic
    try:
        result = db.table(table_id)
    except Exception as err:
        raise SpannerError(f"Could not retrieve table from database '{db.name}'") from err
    if must_exist and not result.exists():
        raise SpannerError(f"Table '{table_id}' must exist in database '{db.name}' but does not")
    return result


def spanner_db(
    *,
    instance_id: str,
    database_id: str,
    project_id: Optional[str] = None,
    creds: Optional[credentials.Credentials] = None,
) -> database.Database:
    """
    Returns the corresponding :py:class:`database.Database` instance.
    The `instance_id` and `database_id` must exist.
    Args:
        instance_id:
        database_id:
        project_id:
        creds:

    Returns:
        Corresponding :py:class:`database.Database` instance.
    Raises:
        SpannerError: On all errors.
    """
    _LOGGER.debug("Getting '%s' using '%s'", database.Database.__name__, locals())
    # validate input
    preprocess.string(database_id, "database_id")
    # logic
    spanner_instance = _spanner_instance(instance_id=instance_id, project_id=project_id, creds=creds)
    try:
        result = spanner_instance.database(database_id=database_id)
        result.reload()
    except Exception as err:
        raise SpannerError(f"Could not get database for ID '{database_id} ins instance ID '{instance_id}'") from err
    if not result.exists():
        raise SpannerError(f"Spanner database for ID '{database_id}' in instance ID '{instance_id}' does not exist.")
    if not result.is_ready():
        raise SpannerError(f"Spanner database for ID '{database_id}' in instance ID '{instance_id}' is not ready.")
    return result


def _spanner_instance(
    *,
    instance_id: str,
    project_id: Optional[str] = None,
    creds: Optional[credentials.Credentials] = None,
) -> instance.Instance:
    # validate input
    preprocess.string(instance_id, "instance_id")
    # logic
    client = _client(project_id=project_id, creds=creds)
    try:
        result = client.instance(instance_id=instance_id)
    except Exception as err:
        raise SpannerError(
            f"Could not get Spanner instance for ID '{instance_id}'. "
            f"Client credentials '{client.credentials}' and project '{client.project_name}'"
        ) from err
    if not result.exists():
        raise SpannerError(
            f"Spanner instance '{instance_id}' does not exist. "
            f"Client credentials '{client.credentials}' and project '{client.project_name}'"
        )
    return result


def _client(*, project_id: Optional[str] = None, creds: Optional[credentials.Credentials] = None) -> spanner.Client:
    # validate input
    preprocess.string(project_id, "project", is_none_valid=True)
    preprocess.validate_type(creds, "creds", credentials.Credentials, is_none_valid=True)
    # logic
    use_emulator = _use_emulator_client()
    try:
        if use_emulator:
            result = _emulator_client(project=project_id, creds=creds)
        else:
            result = _spanner_client(project_id=project_id, creds=creds)
    except Exception as err:
        raise SpannerError(
            f"Could not create client using project '{project_id}', credentials '{creds}', and use emulator '{use_emulator}'"
        ) from err
    return result


def _use_emulator_client() -> bool:
    return os.environ.get(SPANNER_USE_EMULATOR_ENV_VAR, "") == SPANNER_USE_EMULATOR_ENV_VAR_VALUE


def _spanner_client(
    *, project_id: Optional[str] = None, creds: Optional[credentials.Credentials] = None
) -> spanner.Client:
    default_creds, default_project = None, None
    _LOGGER.debug(
        "Creating client '%s' with project '%s' and credentials '%s'", spanner.Client.__name__, project_id, creds
    )
    if project_id is None or creds is None:
        default_creds, default_project = _default_credentials_project()
    if creds is None:
        _LOGGER.debug("Using default credentials '%s' for '%s'", default_creds, spanner.Client.__name__)
        creds = default_creds
    if project_id is None:
        _LOGGER.debug("Using default project '%s' for '%s'", default_project, spanner.Client.__name__)
        project_id = default_project
    return spanner.Client(project=project_id, credentials=creds)


def _default_credentials_project() -> Tuple[credentials.Credentials, str]:
    _LOGGER.debug("Getting default Google Cloud credentials and project ID")
    return auth.default()


def _emulator_client(*args, **kwargs) -> spanner.Client:
    """
    See: https://cloud.google.com/python/docs/reference/spanner/latest/client-usage
    """
    _LOGGER.warning("Ignoring args '%s' and kwargs '%s' because it is a client for the emulator", args, kwargs)
    return spanner.Client(
        project=_SPANNER_EMULATOR_PROJECT_NAME,
        client_options=_SPANNER_EMULATOR_CLIENT_OPTIONS,
        credentials=credentials.AnonymousCredentials(),
    )


def read_table_rows(*, db: database.Database, table_id: str, keys: Set[Any]) -> Generator[Dict[str, Any], None, None]:
    """
    Correct way to use this function::
        db = spanner_db(...)
        table_id = "my_table_id"
        keys = {"my_key_a", "my_key_b"}
        for row in read_table_rows(db=db, table_id=table_id, keys=keys):
            print(row)

    Args:
        db:
        table_id:
        keys:

    Returns:

    """
    _LOGGER.debug("Reading table using '%s'", locals())
    # input validation
    preprocess.validate_type(db, "db", database.Database)
    preprocess.string(table_id, "table_id")
    preprocess.validate_type(keys, "keys", set)
    # logic
    tbl = spanner_table(db=db, table_id=table_id)
    columns = [entry.name for entry in tbl.schema]
    keyset = spanner.KeySet(keys=keys)
    with db.snapshot() as snapshot:
        results: streamed.StreamedResultSet = snapshot.read(table=table_id, keyset=keyset, columns=columns)
        for row in results:
            yield _create_row_from_columns_and_values(columns=columns, values=row)


def _create_row_from_columns_and_values(*, columns: List[str], values: List[Any]) -> Dict[str, Any]:
    result = {}
    for col, val in zip(columns, values):
        result[col] = val
    return result


def upsert_table_row(*, db: database.Database, table_id: str, row: Dict[str, Any]) -> None:
    """
    Will insert or update the ``row``.
    Args:
        db:
        table_id:
        row:

    Returns:

    """
    _LOGGER.debug("Upserting into table using '%s'", locals())
    # input validation
    preprocess.validate_type(db, "db", database.Database)
    preprocess.string(table_id, "table_id")
    preprocess.validate_type(row, "row", dict)
    # logic
    db.run_in_transaction(_upsert_row, table_id, row)
    _LOGGER.debug("Upsert ended successfully for table '%s' in database '%s' and row '%s'", db.name, table_id, row)


def _upsert_row(txn: transaction.Transaction, table_id: str, row: Dict[str, Any]) -> None:
    _LOGGER.debug("Upserting with: %s", locals())
    columns = []
    values = []
    for key, val in row.items():
        columns.append(key)
        values.append(val)
    txn.insert_or_update(table=table_id, columns=columns, values=values)
