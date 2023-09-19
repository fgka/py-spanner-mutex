# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Wrapper around Cloud Spanner Python API, which is a thin layer on top of `Cloud Spanner API`_.

.. _Cloud Spanner API: https://cloud.google.com/python/docs/reference/spanner/latest
"""
import os
from typing import Dict, Optional, Tuple

from google import auth
from google.auth import credentials
from google.cloud import spanner
from google.cloud.spanner_v1 import database, instance

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


def spanner_db(
    *,
    instance_id: str,
    database_id: str,
    project: Optional[str] = None,
    creds: Optional[credentials.Credentials] = None,
) -> database.Database:
    """
    Returns the corresponding :py:class:`database.Database` instance.
    The `instance_id` and `database_id` must exist.
    Args:
        instance_id:
        database_id:
        project:
        creds:

    Returns:
        Corresponding :py:class:`database.Database` instance.
    Raises:
        SpannerError: On all errors.
    """
    # validate input
    preprocess.string(database_id, "database_id")
    # logic
    spanner_instance = _spanner_instance(instance_id=instance_id, project=project, creds=creds)
    try:
        result = spanner_instance.database(database_id=database_id)
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
    project: Optional[str] = None,
    creds: Optional[credentials.Credentials] = None,
) -> instance.Instance:
    # validate input
    preprocess.string(instance_id, "instance_id")
    # logic
    client = _client(project=project, creds=creds)
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


def _client(*, project: Optional[str] = None, creds: Optional[credentials.Credentials] = None) -> spanner.Client:
    # validate input
    preprocess.string(project, "project", is_none_valid=True)
    preprocess.validate_type(creds, "creds", credentials.Credentials, is_none_valid=True)
    # logic
    use_emulator = _use_emulator_client()
    try:
        if use_emulator:
            result = _emulator_client(project=project, creds=creds)
        else:
            result = _spanner_client(project=project, creds=creds)
    except Exception as err:
        raise SpannerError(
            f"Could not create client using project '{project}', credentials '{creds}', and use emulator '{use_emulator}'"
        ) from err
    return result


def _use_emulator_client() -> bool:
    return os.environ.get(SPANNER_USE_EMULATOR_ENV_VAR, "") == SPANNER_USE_EMULATOR_ENV_VAR_VALUE


def _spanner_client(
    *, project: Optional[str] = None, creds: Optional[credentials.Credentials] = None
) -> spanner.Client:
    default_creds, default_project = None, None
    _LOGGER.debug(
        "Creating client '%s' with project '%s' and credentials '%s'", spanner.Client.__name__, project, creds
    )
    if project is None or creds is None:
        default_creds, default_project = _default_credentials_project()
    if creds is None:
        _LOGGER.debug("Using default credentials '%s' for '%s'", default_creds, spanner.Client.__name__)
        creds = default_creds
    if project is None:
        _LOGGER.debug("Using default project '%s' for '%s'", default_project, spanner.Client.__name__)
        project = default_project
    return spanner.Client(project=project, credentials=creds)


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
