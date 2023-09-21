# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,protected-access
# pylint: disable=attribute-defined-outside-init,invalid-name
# type: ignore
from typing import Any, Callable, Generator, List, Optional, Tuple

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
_TEST_TABLE_COLUMN_NAMES: List[str] = ["col_a", "col_b", "col_c"]


class _FieldStub:
    def __init__(self, name: str):
        self.name = name


class _TableStub:
    def __init__(
        self,
        *,
        table_id: str,
        col_names: Optional[List[str]] = None,
        exists: Optional[bool] = True,
        to_raise: Optional[bool] = False,
    ):
        if to_raise:
            raise RuntimeError
        self.table_id = table_id
        self._exists = exists
        col_names = col_names if col_names is not None else _TEST_TABLE_COLUMN_NAMES
        self.schema = []
        for col in col_names:
            self.schema.append(_FieldStub(col))

    def exists(self) -> bool:
        return self._exists


class _SnapshotStub:
    def __init__(self, values: List[List[Any]]):
        self._values = values
        self.calls = {}

    def read(self, table, columns, keyset, **kwargs) -> Generator[List[Any], None, None]:
        if _SnapshotStub.read.__name__ not in self.calls:
            self.calls[_SnapshotStub.read.__name__] = []
        self.calls[_SnapshotStub.read.__name__].append(locals())
        for vals in self._values:
            yield vals


class _SnapshotCtxMngr:
    def __init__(self, values: List[List[Any]]):
        self._values = values

    def __enter__(self):
        return _SnapshotStub(values=self._values)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class _TransactionStub:
    def __init__(self):
        self.called_upsert = []

    def insert_or_update(self, table, columns, values):
        self.called_upsert.append(locals())


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
        snapshot_values: Optional[List[List[Any]]] = None,
        table_columns: Optional[List[str]] = None,
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
        self._snapshot_values = snapshot_values if snapshot_values else []
        self._table_columns = table_columns
        self.called_run_in_txn = []

    def exists(self) -> bool:
        return self._exists

    def is_ready(self) -> bool:
        return self._is_ready

    def table(self, table_id: str) -> Any:
        return _TableStub(
            table_id=table_id, exists=self._table_exists, to_raise=self._table_to_raise, col_names=self._table_columns
        )

    def snapshot(self) -> Any:
        return _SnapshotCtxMngr(values=self._snapshot_values)

    def reload(self) -> None:
        pass

    def run_in_transaction(self, func: Callable, *args, **kw):
        txn = _TransactionStub()
        self.called_run_in_txn.append(locals())
        func(txn, *args, **kw)


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
    db = _create_db()
    table_id = _TEST_TABLE_ID
    # When
    result = gcp_spanner.spanner_table(db=db, table_id=table_id)
    # Then
    assert result is not None
    assert result.table_id == table_id


def _create_db(**db_kwargs):
    if "database_id" not in db_kwargs:
        db_kwargs["database_id"] = _TEST_DATABASE_ID
    if "instance" not in db_kwargs:
        client = _ClientStub(project_id=_TEST_PROJECT_ID, creds=_TEST_CREDS)
        instance = _InstanceStub(client=client, instance_id=_TEST_INSTANCE_ID)
        db_kwargs["instance"] = instance
    return _DatabaseStub(**db_kwargs)


def test_spanner_table_ok_table_does_not_exist():
    # Given
    db = _create_db(table_exists=False)
    table_id = _TEST_TABLE_ID
    # When
    result = gcp_spanner.spanner_table(db=db, table_id=table_id, must_exist=False)
    # Then
    assert result is not None


def test_spanner_table_nok_table_raise():
    # Given
    db = _create_db(table_to_raise=True)
    table_id = _TEST_TABLE_ID
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner.spanner_table(db=db, table_id=table_id)


def test_spanner_table_nok_table_does_not_exist():
    # Given
    db = _create_db(table_exists=False)
    table_id = _TEST_TABLE_ID
    # When/Then
    with pytest.raises(gcp_spanner.SpannerError):
        gcp_spanner.spanner_table(db=db, table_id=table_id, must_exist=True)


_TEST_KEY: str = "TEST_KEY"


def test_read_table_rows_ok_without_results():
    # Given
    db = _create_db()
    table_id = _TEST_TABLE_ID
    keys = {f"{_TEST_KEY}_A", f"{_TEST_KEY}_B", f"{_TEST_KEY}_C"}
    # When
    result = []
    for r in gcp_spanner.read_table_rows(db=db, table_id=table_id, keys=keys):
        result.append(r)
    # Then
    assert not result


def test_read_table_rows_ok_with_results():
    # Given
    columns = ["id", "col_str", "col_int"]
    values = [
        [f"{_TEST_KEY}_A", "value_a", 100],
        [f"{_TEST_KEY}_B", "value_b", 200],
        [f"{_TEST_KEY}_C", "value_c", 300],
    ]
    values_dict = {val[0]: val[1:] for val in values}
    db = _create_db(table_columns=columns, snapshot_values=values)
    table_id = _TEST_TABLE_ID
    keys = {val[0] for val in values}
    # When
    result = []
    for r in gcp_spanner.read_table_rows(db=db, table_id=table_id, keys=keys):
        result.append(r)
    # Then
    assert len(result) == len(values)
    for r in result:
        id = r.get("id")
        exp_vals = values_dict.get(id)
        assert exp_vals
        for ndx in range(1, len(columns)):
            assert r.get(columns[ndx]) == exp_vals[ndx - 1]


def test_upsert_table_row_ok():
    # Given
    row = {"id": _TEST_KEY, "col_str": "str_value", "col_int": 123}
    db = _create_db()
    table_id = _TEST_TABLE_ID
    # When
    gcp_spanner.conditional_upsert_table_row(db=db, table_id=table_id, row=row)
    # Then
    assert len(db.called_run_in_txn) == 1
