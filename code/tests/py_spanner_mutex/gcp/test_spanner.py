# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,protected-access
# pylint: disable=attribute-defined-outside-init,invalid-name
# type: ignore
from typing import Any, Callable, Optional, Tuple

import pytest
from google.auth import credentials
from google.cloud import spanner
from google.cloud.spanner_v1 import database

from py_spanner_mutex.gcp import spanner as gcp_spanner


class _CloudCredentials(credentials.Credentials):
    def refresh(self, request):
        raise RuntimeError


_TEST_PROJECT_ID: str = "TEST_PROJECT"
_TEST_CREDS: credentials.Credentials = _CloudCredentials()


def test__emulator_client_ok_without_args():
    # Given/When
    obj = gcp_spanner._emulator_client()
    # Then
    assert isinstance(obj, spanner.Client)
    assert obj.project_name.endswith(gcp_spanner._SPANNER_EMULATOR_PROJECT_NAME)
    assert isinstance(obj.credentials, credentials.AnonymousCredentials)


def test__emulator_client_ok_with_args():
    # Given/When
    obj = gcp_spanner._emulator_client(project_id=_TEST_PROJECT_ID, creds=_TEST_CREDS)
    # Then
    assert isinstance(obj, spanner.Client)
    # Then: ignores project argument
    assert obj.project_name.endswith(gcp_spanner._SPANNER_EMULATOR_PROJECT_NAME)
    assert obj.project_name != _TEST_PROJECT_ID
    # Then: ignores credentials argument
    assert obj.credentials != _TEST_CREDS
    assert isinstance(obj.credentials, credentials.AnonymousCredentials)


@pytest.fixture
def default_credentials_project(
    project_id: str = _TEST_PROJECT_ID, creds: credentials.Credentials = _TEST_CREDS
) -> Callable[[], Tuple[credentials.Credentials, str]]:
    def default_credentials_project_mock() -> Tuple[credentials.Credentials, str]:
        return creds, project_id

    return default_credentials_project_mock


def test__spanner_client_ok_with_args(monkeypatch, default_credentials_project):
    # Given
    project_id = _TEST_PROJECT_ID + "_LOCAL"
    creds = _CloudCredentials()
    monkeypatch.setattr(gcp_spanner, gcp_spanner._default_credentials_project.__name__, default_credentials_project)
    # When
    result = gcp_spanner._spanner_client(project_id=project_id, creds=creds)
    # Then
    assert isinstance(result, spanner.Client)
    assert result.project_name.endswith(project_id)
    assert result.credentials == creds


def test__spanner_client_ok_without_args(monkeypatch, default_credentials_project):
    # Given
    monkeypatch.setattr(gcp_spanner, gcp_spanner._default_credentials_project.__name__, default_credentials_project)
    # When
    result = gcp_spanner._spanner_client()
    # Then
    assert isinstance(result, spanner.Client)
    assert result.project_name.endswith(_TEST_PROJECT_ID)
    assert result.credentials == _TEST_CREDS


def test__client_ok_with_args_emulator(monkeypatch, default_credentials_project):
    # Given
    project_id = _TEST_PROJECT_ID + "_LOCAL"
    creds = _CloudCredentials()
    monkeypatch.setattr(gcp_spanner, gcp_spanner._default_credentials_project.__name__, default_credentials_project)
    monkeypatch.setenv(gcp_spanner.SPANNER_USE_EMULATOR_ENV_VAR, gcp_spanner.SPANNER_USE_EMULATOR_ENV_VAR_VALUE)
    # When
    result = gcp_spanner._client(project_id=project_id, creds=creds)
    # Then
    assert isinstance(result, spanner.Client)
    # Then: ignore arguments, it is for emulator
    assert result.credentials != creds
    assert not result.project_name.endswith(project_id)


def test__client_ok_with_args_not_emulator(monkeypatch, default_credentials_project):
    # Given
    project_id = _TEST_PROJECT_ID + "_LOCAL"
    creds = _CloudCredentials()
    monkeypatch.setattr(gcp_spanner, gcp_spanner._default_credentials_project.__name__, default_credentials_project)
    monkeypatch.setenv(
        gcp_spanner.SPANNER_USE_EMULATOR_ENV_VAR, gcp_spanner.SPANNER_USE_EMULATOR_ENV_VAR_VALUE + "_NOT"
    )
    # When
    result = gcp_spanner._client(project_id=project_id, creds=creds)
    # Then
    assert isinstance(result, spanner.Client)
    # Then: ignore arguments, it is for emulator
    assert result.credentials == creds
    assert result.project_name.endswith(project_id)


@pytest.mark.parametrize("use_emulator", [True, False])
def test__client_nok_raise(monkeypatch, use_emulator: bool):
    # Given
    def client_mock(*args, **kwargs) -> Any:
        raise RuntimeError

    monkeypatch.setattr(gcp_spanner, gcp_spanner._emulator_client.__name__, client_mock)
    monkeypatch.setattr(gcp_spanner, gcp_spanner._spanner_client.__name__, client_mock)
    env_var_value = gcp_spanner.SPANNER_USE_EMULATOR_ENV_VAR_VALUE if use_emulator else "NOT"
    monkeypatch.setenv(gcp_spanner.SPANNER_USE_EMULATOR_ENV_VAR, env_var_value)
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner._client()


_TEST_INSTANCE_ID: str = "TEST_INSTANCE_ID"
_TEST_DATABASE_ID: str = "TEST_DATABASE_ID"
_TEST_TABLE_ID: str = "TEST_TABLE_ID"


class _TableStub:
    def __init__(self, *, table_id: str, exists: Optional[bool] = True, to_raise: Optional[bool] = False):
        if to_raise:
            raise RuntimeError
        self.table_id = table_id
        self._exists = exists

    def exists(self) -> bool:
        return self._exists


class _DatabaseStub(database.Database):
    def __init__(
        self,
        *,
        instance: Any,
        database_id: str,
        exists: Optional[bool] = True,
        is_ready: Optional[bool] = True,
        to_raise: Optional[bool] = False,
        table_exists: Optional[bool] = True,
        table_to_raise: Optional[bool] = False,
    ):
        if to_raise:
            raise RuntimeError
        super().__init__(database_id=database_id, instance=instance)
        self.instance = instance
        self.database_id = database_id
        self._exists = exists
        self._is_ready = is_ready
        self._table_exists = table_exists
        self._table_to_raise = table_to_raise

    def exists(self) -> bool:
        return self._exists

    def is_ready(self) -> bool:
        return self._is_ready

    def table(self, table_id: str) -> Any:
        return _TableStub(table_id=table_id, exists=self._table_exists, to_raise=self._table_to_raise)


class _InstanceStub:
    def __init__(
        self,
        *,
        client: Any,
        instance_id: str,
        exists: Optional[bool] = True,
        to_raise: Optional[bool] = False,
        database_exists: Optional[bool] = True,
        database_is_ready: Optional[bool] = True,
        database_to_raise: Optional[bool] = False,
    ):
        if to_raise:
            raise RuntimeError
        self._client = client
        self.name = f"{client.project_name}/instances/{instance_id}"
        self._exists = exists
        self._database_exists = database_exists
        self._database_is_ready = database_is_ready
        self._database_to_raise = database_to_raise

    def exists(self) -> bool:
        return self._exists

    def database(self, database_id: str) -> Any:
        return _DatabaseStub(
            instance=self,
            database_id=database_id,
            exists=self._database_exists,
            is_ready=self._database_is_ready,
            to_raise=self._database_to_raise,
        )


class _ClientStub:
    def __init__(
        self,
        *,
        project_id: str = None,
        creds: credentials.Credentials = None,
        instance_exists: Optional[bool] = True,
        instance_to_raise: Optional[bool] = False,
        database_exists: Optional[bool] = True,
        database_is_ready: Optional[bool] = True,
        database_to_raise: Optional[bool] = False,
    ):
        self.project = project_id if project_id is not None else _TEST_PROJECT_ID
        self.credentials = creds if creds is not None else _TEST_CREDS
        self.project_name = f"projects/{self.project}"
        self._instance_exists = instance_exists
        self._instance_to_raise = instance_to_raise
        self._database_exists = database_exists
        self._database_is_ready = database_is_ready
        self._database_to_raise = database_to_raise
        # compatibility only
        self.route_to_leader_enabled = False

    def instance(self, instance_id: str) -> Any:
        return _InstanceStub(
            client=self,
            instance_id=instance_id,
            exists=self._instance_exists,
            to_raise=self._instance_to_raise,
            database_exists=self._database_exists,
            database_is_ready=self._database_is_ready,
            database_to_raise=self._database_to_raise,
        )


def test__spanner_instance_ok_with_args(monkeypatch):
    # Given
    project_id_arg = _TEST_PROJECT_ID + "_LOCAL"
    creds_arg = _CloudCredentials()
    instance_id = _TEST_INSTANCE_ID
    client = _ClientStub(project_id=project_id_arg, creds=creds_arg)

    def client_mock(*, project_id: Optional[str] = None, creds: Optional[credentials.Credentials] = None) -> Any:
        assert project_id == project_id_arg
        assert creds == creds_arg
        return client

    monkeypatch.setattr(gcp_spanner, gcp_spanner._client.__name__, client_mock)
    # When
    result = gcp_spanner._spanner_instance(instance_id=instance_id, project_id=project_id_arg, creds=creds_arg)
    # Then
    assert result is not None


def test__spanner_instance_nok_instance_raises(monkeypatch):
    # Given
    project_id_arg = _TEST_PROJECT_ID + "_LOCAL"
    creds_arg = _CloudCredentials()
    instance_id = _TEST_INSTANCE_ID
    client = _ClientStub(project_id=project_id_arg, creds=creds_arg, instance_to_raise=True)

    def client_mock(*, project_id: Optional[str] = None, creds: Optional[credentials.Credentials] = None) -> Any:
        assert project_id == project_id_arg
        assert creds == creds_arg
        return client

    monkeypatch.setattr(gcp_spanner, gcp_spanner._client.__name__, client_mock)
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner._spanner_instance(instance_id=instance_id, project_id=project_id_arg, creds=creds_arg)


def test__spanner_instance_nok_instance_does_not_exist(monkeypatch):
    # Given
    project_id_arg = _TEST_PROJECT_ID + "_LOCAL"
    creds_arg = _CloudCredentials()
    instance_id = _TEST_INSTANCE_ID
    client = _ClientStub(project_id=project_id_arg, creds=creds_arg, instance_exists=False)

    def client_mock(*, project_id: Optional[str] = None, creds: Optional[credentials.Credentials] = None) -> Any:
        assert project_id == project_id_arg
        assert creds == creds_arg
        return client

    monkeypatch.setattr(gcp_spanner, gcp_spanner._client.__name__, client_mock)
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner._spanner_instance(instance_id=instance_id, project_id=project_id_arg, creds=creds_arg)


def test_spanner_db_ok(monkeypatch):
    # Given
    project_id_arg = _TEST_PROJECT_ID
    creds_arg = _CloudCredentials()
    instance_id_arg = _TEST_INSTANCE_ID
    database_id = _TEST_DATABASE_ID
    spanner_instance = _InstanceStub(
        client=_ClientStub(project_id=project_id_arg, creds=creds_arg), instance_id=instance_id_arg
    )

    def spanner_instance_mock(
        *,
        instance_id: str,
        project_id: Optional[str] = None,
        creds: Optional[credentials.Credentials] = None,
    ) -> Any:
        assert project_id == project_id_arg
        assert creds == creds_arg
        assert instance_id == instance_id_arg
        return spanner_instance

    monkeypatch.setattr(gcp_spanner, gcp_spanner._spanner_instance.__name__, spanner_instance_mock)
    # When
    result = gcp_spanner.spanner_db(
        instance_id=instance_id_arg, database_id=database_id, project_id=project_id_arg, creds=creds_arg
    )
    # Then
    assert result is not None
    assert _TEST_PROJECT_ID in result.name
    assert _TEST_INSTANCE_ID in result.name
    assert _TEST_DATABASE_ID in result.name


def test_spanner_db_nok_database_raises(monkeypatch):
    # Given
    project_id_arg = _TEST_PROJECT_ID
    creds_arg = _CloudCredentials()
    instance_id_arg = _TEST_INSTANCE_ID
    database_id = _TEST_DATABASE_ID
    spanner_instance = _InstanceStub(
        client=_ClientStub(project_id=project_id_arg, creds=creds_arg),
        instance_id=instance_id_arg,
        database_to_raise=True,
    )

    def spanner_instance_mock(
        *,
        instance_id: str,
        project_id: Optional[str] = None,
        creds: Optional[credentials.Credentials] = None,
    ) -> Any:
        assert project_id == project_id_arg
        assert creds == creds_arg
        assert instance_id == instance_id_arg
        return spanner_instance

    monkeypatch.setattr(gcp_spanner, gcp_spanner._spanner_instance.__name__, spanner_instance_mock)
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner.spanner_db(
            instance_id=instance_id_arg, database_id=database_id, project_id=project_id_arg, creds=creds_arg
        )


def test_spanner_db_nok_database_does_not_exist(monkeypatch):
    # Given
    project_id_arg = _TEST_PROJECT_ID
    creds_arg = _CloudCredentials()
    instance_id_arg = _TEST_INSTANCE_ID
    database_id = _TEST_DATABASE_ID
    spanner_instance = _InstanceStub(
        client=_ClientStub(project_id=project_id_arg, creds=creds_arg),
        instance_id=instance_id_arg,
        database_exists=False,
    )

    def spanner_instance_mock(
        *,
        instance_id: str,
        project_id: Optional[str] = None,
        creds: Optional[credentials.Credentials] = None,
    ) -> Any:
        assert project_id == project_id_arg
        assert creds == creds_arg
        assert instance_id == instance_id_arg
        return spanner_instance

    monkeypatch.setattr(gcp_spanner, gcp_spanner._spanner_instance.__name__, spanner_instance_mock)
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner.spanner_db(
            instance_id=instance_id_arg, database_id=database_id, project_id=project_id_arg, creds=creds_arg
        )


def test_spanner_db_nok_database_is_not_ready(monkeypatch):
    # Given
    project_id_arg = _TEST_PROJECT_ID
    creds_arg = _CloudCredentials()
    instance_id_arg = _TEST_INSTANCE_ID
    database_id = _TEST_DATABASE_ID
    spanner_instance = _InstanceStub(
        client=_ClientStub(project_id=project_id_arg, creds=creds_arg),
        instance_id=instance_id_arg,
        database_is_ready=False,
    )

    def spanner_instance_mock(
        *,
        instance_id: str,
        project_id: Optional[str] = None,
        creds: Optional[credentials.Credentials] = None,
    ) -> Any:
        assert project_id == project_id_arg
        assert creds == creds_arg
        assert instance_id == instance_id_arg
        return spanner_instance

    monkeypatch.setattr(gcp_spanner, gcp_spanner._spanner_instance.__name__, spanner_instance_mock)
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner.spanner_db(
            instance_id=instance_id_arg, database_id=database_id, project_id=project_id_arg, creds=creds_arg
        )


def test_spanner_table_ok():
    # Given
    db = _DatabaseStub(
        instance=_InstanceStub(
            client=_ClientStub(project_id=_TEST_PROJECT_ID, creds=_TEST_CREDS), instance_id=_TEST_INSTANCE_ID
        ),
        database_id=_TEST_DATABASE_ID,
    )
    table_id = _TEST_TABLE_ID
    # When
    result = gcp_spanner.spanner_table(db=db, table_id=table_id)
    # Then
    assert result is not None
    assert result.table_id == table_id


def test_spanner_table_ok_table_does_not_exist():
    # Given
    db = _DatabaseStub(
        instance=_InstanceStub(
            client=_ClientStub(project_id=_TEST_PROJECT_ID, creds=_TEST_CREDS), instance_id=_TEST_INSTANCE_ID
        ),
        database_id=_TEST_DATABASE_ID,
        table_exists=False,
    )
    table_id = _TEST_TABLE_ID
    # When
    result = gcp_spanner.spanner_table(db=db, table_id=table_id, must_exist=False)
    # Then
    assert result is not None


def test_spanner_table_nok_table_raise():
    # Given
    db = _DatabaseStub(
        instance=_InstanceStub(
            client=_ClientStub(project_id=_TEST_PROJECT_ID, creds=_TEST_CREDS), instance_id=_TEST_INSTANCE_ID
        ),
        database_id=_TEST_DATABASE_ID,
        table_to_raise=True,
    )
    table_id = _TEST_TABLE_ID
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner.spanner_table(db=db, table_id=table_id)


def test_spanner_table_nok_table_does_not_exist():
    # Given
    db = _DatabaseStub(
        instance=_InstanceStub(
            client=_ClientStub(project_id=_TEST_PROJECT_ID, creds=_TEST_CREDS), instance_id=_TEST_INSTANCE_ID
        ),
        database_id=_TEST_DATABASE_ID,
        table_exists=False,
    )
    table_id = _TEST_TABLE_ID
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner.spanner_table(db=db, table_id=table_id, must_exist=True)
