# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,protected-access
# pylint: disable=attribute-defined-outside-init,invalid-name
# type: ignore
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

import pytest
from google.auth import credentials
from google.cloud import spanner
from google.cloud.spanner_v1 import database, table, transaction

from py_spanner_mutex import spanner_mutex
from py_spanner_mutex.dto import mutex


class MySpannerMutex(spanner_mutex.SpannerMutex):
    def __init__(
        self,
        *,
        config: mutex.MutexConfig,
        client_uuid: Optional[uuid.UUID] = None,
        client_display_name: Optional[str] = None,
        creds: Optional[credentials.Credentials] = None,
    ):
        super(MySpannerMutex, self).__init__(
            config=config, client_uuid=client_uuid, client_display_name=client_display_name, creds=creds
        )
        self.is_mutex_needed_value = True
        self.execute_critical_section_sleep_in_secs = 0
        self.called = {
            self.__class__.is_mutex_needed.__name__: [],
            self.__class__.execute_critical_section.__name__: [],
        }

    def is_mutex_needed(self) -> bool:
        self.called[self.__class__.is_mutex_needed.__name__].append(True)
        return self.is_mutex_needed_value

    def execute_critical_section(self, max_end_time: datetime) -> None:
        self.called[self.__class__.execute_critical_section.__name__].append(locals())
        time.sleep(self.execute_critical_section_sleep_in_secs)


_TEST_CONFIG: mutex.MutexConfig = mutex.MutexConfig(
    mutex_uuid=uuid.uuid4(),
    instance_id="INSTANCE_ID",
    database_id="DATABASE_ID",
    table_id="TABLE_ID",
)
_TEST_CLIENT_UUID: uuid.UUID = uuid.uuid4()
_TEST_CLIENT_DISPLAY_NAME: str = "CLIENT_DISPLAY_NAME"


class TestSpannerMutex:
    def setup_method(self):
        self.obj = MySpannerMutex(
            config=_TEST_CONFIG, client_uuid=_TEST_CLIENT_UUID, client_display_name=_TEST_CLIENT_DISPLAY_NAME
        )

    def test_members_ok(self):
        assert self.obj.config == _TEST_CONFIG
        assert self.obj.client_uuid == _TEST_CLIENT_UUID
        assert self.obj.client_display_name == _TEST_CLIENT_DISPLAY_NAME

    def test_validate_ok(self, monkeypatch):
        # Given
        called = None

        def mocked_spanner_table(*args, **kwargs) -> Any:
            nonlocal called
            called = kwargs
            return "TABLE"

        monkeypatch.setattr(spanner_mutex, spanner_mutex._spanner_table.__name__, mocked_spanner_table)
        # When/Then
        assert self.obj.validate()
        # Then: spanner_table
        assert called is not None
        assert called.get("instance_id") == self.obj.config.instance_id
        assert called.get("database_id") == self.obj.config.database_id
        assert called.get("table_id") == self.obj.config.table_id
        assert called.get("must_exist")

    def test_start_ok_single_no_previous_state(self, monkeypatch):
        # Given
        self.obj.is_mutex_needed_value = True
        spanner_called = _mock_spanner_module(monkeypatch)
        # When
        self.obj.start()
        # Then: is_mutex_needed
        is_mutex_needed = self.obj.called.get(self.obj.__class__.is_mutex_needed.__name__)
        assert len(is_mutex_needed) == 1
        # Then: execute_critical_section
        execute_critical_section = self.obj.called.get(self.obj.__class__.execute_critical_section.__name__)
        assert len(execute_critical_section) == 1
        # Then: spanner_called: spanner_db
        called_spanner_db = spanner_called.get(spanner_mutex.spanner.spanner_db.__name__)
        assert len(called_spanner_db) == 1
        # Then: spanner_called: spanner_table
        called_spanner_table = spanner_called.get(spanner_mutex.spanner.spanner_table.__name__)
        assert len(called_spanner_table) == 1
        # Then: spanner_called: read_table_rows
        called_read_table_rows = spanner_called.get(spanner_mutex.spanner.read_table_rows.__name__)
        assert len(called_read_table_rows) == 1
        kwargs_read_table_rows = called_read_table_rows[0].get("kwargs", {})
        assert kwargs_read_table_rows.get("table_id") == self.obj.config.table_id
        kwargs_keys_read_table_rows = list(kwargs_read_table_rows.get("keys"))
        assert len(kwargs_keys_read_table_rows) == 1
        assert kwargs_keys_read_table_rows[0][0] == str(self.obj.config.mutex_uuid)
        # Then: spanner_called: conditional_upsert_table_row
        called_conditional_upsert_table_row = spanner_called.get(
            spanner_mutex.spanner.conditional_upsert_table_row.__name__
        )
        assert len(called_conditional_upsert_table_row) == 2  # one to set and one to release the mutex
        for el in called_conditional_upsert_table_row:
            kwargs = el.get("kwargs", {})
            assert kwargs.get("table_id") == self.obj.config.table_id
            kwargs_row = kwargs.get("row", {})
            assert kwargs_row.get("uuid") == str(self.obj.config.mutex_uuid)
            assert kwargs_row.get("update_client_uuid") == str(self.obj.client_uuid)
            assert kwargs_row.get("update_client_display_name") == self.obj.client_display_name
        # Then: spanner_called: read_in_transaction
        called_read_in_transaction = spanner_called.get(spanner_mutex.spanner.read_in_transaction.__name__)
        assert len(called_read_in_transaction) == 2  # one to set and one to release the mutex
        for el in called_read_in_transaction:
            kwargs_keys = el.get("kwargs", {}).get("keys")
            assert len(kwargs_keys) == 1
            assert list(kwargs_keys)[0][0] == str(self.obj.config.mutex_uuid)

    def test_start_ok_single_with_previous_state(self, monkeypatch):
        # Given
        self.obj.is_mutex_needed_value = True
        spanner_called = _mock_spanner_module(monkeypatch)
        # When
        self.obj.start()
        # Then: is_mutex_needed
        # TODO


def _mock_spanner_module(
    monkeypatch,
    *,
    read_table_rows: List[Any] = None,
    read_in_transaction: List[Any] = None,
    conditional_upsert_table_row: bool = True,
    spanner_table: Optional[Any] = None,
    spanner_db: Optional[Any] = None,
    can_upsert_args: Optional[Any] = None,
) -> Dict[str, List[Any]]:
    result = {}
    read_table_rows = read_table_rows if read_table_rows else iter([])
    read_in_transaction = read_in_transaction if read_in_transaction else iter([])
    can_upsert_args = can_upsert_args if can_upsert_args else [None, None, None]

    def _add_to_result(key: str, value: Any) -> None:
        nonlocal result
        where = result.get(key)
        if where is None:
            where = []
            result[key] = where
        where.append(value)

    def mocked_read_table_rows(*args, **kwargs) -> Any:
        _add_to_result(spanner_mutex.spanner.read_table_rows.__name__, locals())
        return read_table_rows

    def mocked_read_in_transaction(*args, **kwargs) -> Any:
        _add_to_result(spanner_mutex.spanner.read_in_transaction.__name__, locals())
        return read_in_transaction

    def mocked_conditional_upsert_table_row(*args, **kwargs) -> Any:
        _add_to_result(spanner_mutex.spanner.conditional_upsert_table_row.__name__, locals())
        if "can_upsert" in kwargs:
            kwargs.get("can_upsert")(*can_upsert_args)
        return conditional_upsert_table_row

    def mocked_spanner_table(*args, **kwargs) -> Any:
        _add_to_result(spanner_mutex.spanner.spanner_table.__name__, locals())
        return spanner_table

    def mocked_spanner_db(*args, **kwargs) -> Any:
        _add_to_result(spanner_mutex.spanner.spanner_db.__name__, locals())
        return spanner_db

    monkeypatch.setattr(spanner_mutex.spanner, spanner_mutex.spanner.read_table_rows.__name__, mocked_read_table_rows)
    monkeypatch.setattr(
        spanner_mutex.spanner, spanner_mutex.spanner.read_in_transaction.__name__, mocked_read_in_transaction
    )
    monkeypatch.setattr(
        spanner_mutex.spanner,
        spanner_mutex.spanner.conditional_upsert_table_row.__name__,
        mocked_conditional_upsert_table_row,
    )
    monkeypatch.setattr(spanner_mutex.spanner, spanner_mutex.spanner.spanner_table.__name__, mocked_spanner_table)
    monkeypatch.setattr(spanner_mutex.spanner, spanner_mutex.spanner.spanner_db.__name__, mocked_spanner_db)

    return result
