# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,protected-access
# pylint: disable=attribute-defined-outside-init,invalid-name
# type: ignore
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytest
import pytz
from google.auth import credentials

from py_spanner_mutex import spanner_mutex
from py_spanner_mutex.dto import mutex

_DATETIME_UTC_NOW: datetime = datetime.utcnow().replace(tzinfo=pytz.UTC)
# pinning 'NOW'
spanner_mutex.datetime_helper._datetime_utcnow = lambda: _DATETIME_UTC_NOW
mutex.datetime_helper._datetime_utcnow = lambda: _DATETIME_UTC_NOW


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
    mutex_max_retries=mutex.MIN_MUTEX_MAX_RETRIES,
    mutex_wait_time_in_secs=mutex.MIN_MUTEX_WAIT_TIME_IN_SECONDS,
)
_TEST_CLIENT_UUID: uuid.UUID = uuid.uuid4()
_TEST_CLIENT_DISPLAY_NAME: str = "CLIENT_DISPLAY_NAME"
_TEST_MUTEX_STATE: mutex.MutexState = mutex.MutexState(
    uuid=_TEST_CONFIG.mutex_uuid,
    display_name="TEST_MUTEX_STATE",
    status=mutex.MutexStatus.DONE,
    update_time_utc=_DATETIME_UTC_NOW,
    update_client_uuid=_TEST_CLIENT_UUID,
    update_client_display_name=_TEST_CLIENT_DISPLAY_NAME,
)


def _mutex_state(*, is_stale: bool = False, breach_watermark: bool = False, breaches_jitter: bool = False, **kwargs):
    state_kw = _TEST_MUTEX_STATE.as_dict()
    update_time_utc = None
    if is_stale:
        update_time_utc = _DATETIME_UTC_NOW - timedelta(seconds=_TEST_CONFIG.mutex_staleness_in_secs + 1)
    elif breach_watermark:
        ttl_plus_max_jitter_in_secs = (
            _TEST_CONFIG.mutex_ttl_in_secs + spanner_mutex._MUTEX_TTL_JITTER_IN_PERCENT * _TEST_CONFIG.mutex_ttl_in_secs
        )
        update_time_utc = _DATETIME_UTC_NOW - timedelta(seconds=ttl_plus_max_jitter_in_secs + 1)
    elif breaches_jitter:
        max_jitter_in_secs = _TEST_CONFIG.mutex_ttl_in_secs * spanner_mutex._MUTEX_TTL_JITTER_IN_PERCENT
        update_time_utc = _DATETIME_UTC_NOW - timedelta(seconds=max_jitter_in_secs + 1)
    elif "update_time_utc" not in kwargs:
        update_time_utc = _DATETIME_UTC_NOW
    if update_time_utc is not None:
        kwargs["update_time_utc"] = update_time_utc
    for key, val in kwargs.items():
        if key in state_kw:
            state_kw[key] = val
    return mutex.MutexState(**state_kw)


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

    def test_validate_nok_no_raise(self, monkeypatch):
        # Given
        def mocked_spanner_table(*args, **kwargs) -> Any:
            raise RuntimeError

        monkeypatch.setattr(spanner_mutex, spanner_mutex._spanner_table.__name__, mocked_spanner_table)
        # When/Then
        assert not self.obj.validate(raise_if_invalid=False)

    def test_validate_nok_raise(self, monkeypatch):
        # Given
        def mocked_spanner_table(*args, **kwargs) -> Any:
            raise RuntimeError

        monkeypatch.setattr(spanner_mutex, spanner_mutex._spanner_table.__name__, mocked_spanner_table)
        # When/Then
        with pytest.raises(spanner_mutex.SpannerMutexError):
            self.obj.validate(raise_if_invalid=True)

    def test_status_ok(self, monkeypatch):
        # Given
        cur_state = _TEST_MUTEX_STATE
        spanner_called = _mock_spanner_module(
            monkeypatch, read_table_rows=[cur_state.to_spanner_row(with_commit_ts=False)]
        )
        # When
        result = self.obj.status
        # Then
        assert result == cur_state.status
        assert len(spanner_called.get(spanner_mutex.spanner.read_table_rows.__name__)) == 1

    def test__is_state_stale_ok_state_is_none_true(self):
        # Given
        state = None
        # When/Then
        assert self.obj._is_state_stale(state)

    def test__is_state_stale_ok_state_is_old_true(self):
        # Given
        for status in mutex.MutexStatus:
            state = _mutex_state(is_stale=True, status=status)
            # When/Then
            assert self.obj._is_state_stale(state)

    def test__is_state_stale_ok_state_is_current_false(self):
        # Given
        for status in mutex.MutexStatus:
            state = _mutex_state(status=status)
            # When/Then
            assert not self.obj._is_state_stale(state)

    def test__is_critical_section_status_or_started_ok_none(self):
        # Given/When/Then
        for status in mutex.MutexStatus:
            assert not self.obj._is_critical_section_status(None, status)

    def test__is_critical_section_status_or_started_ok_different(self):
        # Given
        status = mutex.MutexStatus.FAILED
        # When/Then
        assert not self.obj._is_critical_section_status(_mutex_state(status=mutex.MutexStatus.UNKNOWN), status)

    def test__is_critical_section_status_or_started_ok_all_states(self):
        # Given
        for status in mutex.MutexStatus:
            state = _mutex_state(status=status)
            # When/Then
            assert self.obj._is_critical_section_status(state, status)

    def test__is_watermark_breached_ok_state_none_breaches(self):
        # Given/When/Then
        assert self.obj._is_watermark_breached(None)

    def test__is_watermark_breached_ok_state_current_does_not_breach(self):
        # Given
        state = _mutex_state()
        # When/Then
        assert not self.obj._is_watermark_breached(state)

    def test__is_watermark_breached_ok_state_old_breaches(self):
        # Given
        max_jitter_in_secs = self.obj.config.mutex_ttl_in_secs * spanner_mutex._MUTEX_TTL_JITTER_IN_PERCENT
        state = _mutex_state(breach_watermark=True)
        # When/Then
        assert self.obj._is_watermark_breached(state)

    def test__is_watermark_breached_ok_just_jitter(self):
        # Given
        state = _mutex_state(breaches_jitter=True)
        # When/Then
        assert self.obj._is_watermark_breached(state, just_jitter=True)
        assert not self.obj._is_watermark_breached(state, just_jitter=False)

    @pytest.mark.parametrize(
        "state",
        [
            _mutex_state(  # state not stale and DONE
                breach_watermark=True,
                status=mutex.MutexStatus.DONE,
            ),
            _mutex_state(status=mutex.MutexStatus.STARTED),  # STARTED and doesn't breach watermark
            _mutex_state(  # STARTED and doesn't breach watermark, just jitter
                breaches_jitter=True,
                status=mutex.MutexStatus.STARTED,
            ),
            _mutex_state(status=mutex.MutexStatus.STARTED),  # status is not relevant
            _mutex_state(status=mutex.MutexStatus.UNKNOWN),  # state does not breach watermark
        ],
    )
    def test__should_try_to_acquire_mutex_ok_false(self, state: mutex.MutexState):
        # Given/When/Then
        assert not self.obj._should_try_to_acquire_mutex(state)

    @pytest.mark.parametrize(
        "state",
        [
            None,  # no state
            _mutex_state(  # state is stale
                is_stale=True,
                status=mutex.MutexStatus.STARTED,
            ),
            _mutex_state(  # status is relevant and watermark is breached
                breach_watermark=True,
                status=mutex.MutexStatus.UNKNOWN,
            ),
            _mutex_state(  # status is relevant and watermark is breached
                breach_watermark=True,
                status=mutex.MutexStatus.FAILED,
            ),
        ],
    )
    def test__should_try_to_acquire_mutex_ok_true(self, state: mutex.MutexState):
        # Given/When/Then
        assert self.obj._should_try_to_acquire_mutex(state)

    @pytest.mark.parametrize(
        "existing_state,target_state, expected",
        [
            #### Same client UUID
            ### happy path: True
            # first mutex
            (None, _mutex_state(status=mutex.MutexStatus.STARTED), True),
            # change state
            (_mutex_state(status=mutex.MutexStatus.UNKNOWN), _mutex_state(status=mutex.MutexStatus.STARTED), True),
            (_mutex_state(status=mutex.MutexStatus.FAILED), _mutex_state(status=mutex.MutexStatus.STARTED), True),
            (_mutex_state(status=mutex.MutexStatus.STARTED), _mutex_state(status=mutex.MutexStatus.DONE), True),
            (_mutex_state(status=mutex.MutexStatus.DONE), _mutex_state(status=mutex.MutexStatus.STARTED), True),
            ### Happy path: False
            # same state
            (_mutex_state(status=mutex.MutexStatus.DONE), _mutex_state(status=mutex.MutexStatus.DONE), False),
            #### Different client UUID
            ### Happy path: True
            # Existing is stale
            (
                _mutex_state(
                    is_stale=True,
                    status=mutex.MutexStatus.DONE,
                    update_client_uuid=uuid.uuid4(),
                ),
                _mutex_state(status=mutex.MutexStatus.STARTED),
                True,
            ),
            # Existing breaches watermark
            (
                _mutex_state(
                    breach_watermark=True,
                    status=mutex.MutexStatus.STARTED,
                    update_client_uuid=uuid.uuid4(),
                ),
                _mutex_state(status=mutex.MutexStatus.STARTED),
                True,
            ),
            # Existing is FAILED and breaches jitter
            (
                _mutex_state(breaches_jitter=True, status=mutex.MutexStatus.FAILED, update_client_uuid=uuid.uuid4()),
                _mutex_state(status=mutex.MutexStatus.STARTED),
                True,
            ),
            ### Happy path: False
            # Already done
            (
                _mutex_state(breaches_jitter=True, status=mutex.MutexStatus.DONE, update_client_uuid=uuid.uuid4()),
                _mutex_state(status=mutex.MutexStatus.STARTED),
                False,
            ),
            # Already started
            (
                _mutex_state(breaches_jitter=True, status=mutex.MutexStatus.STARTED, update_client_uuid=uuid.uuid4()),
                _mutex_state(status=mutex.MutexStatus.STARTED),
                False,
            ),
            # Failed but does not breach jitter
            (
                _mutex_state(status=mutex.MutexStatus.FAILED, update_client_uuid=uuid.uuid4()),
                _mutex_state(status=mutex.MutexStatus.STARTED),
                False,
            ),
        ],
    )
    def test__set_mutex_ok(
        self, monkeypatch, existing_state: mutex.MutexState, target_state: mutex.MutexState, expected: bool
    ):
        # Given
        read_in_transaction = (
            [existing_state.to_spanner_row(with_commit_ts=False)] if existing_state is not None else None
        )
        can_upsert_args = (None, None, target_state.to_spanner_row(with_commit_ts=False))
        spanner_called = _mock_spanner_module(
            monkeypatch, read_in_transaction=read_in_transaction, can_upsert_args=can_upsert_args
        )
        # When
        result = self.obj._set_mutex(target_state)
        # Then
        assert result == expected
        # Then: behavior
        assert len(spanner_called.get(spanner_mutex.spanner.read_in_transaction.__name__)) == 1
        assert len(spanner_called.get(spanner_mutex.spanner.conditional_upsert_table_row.__name__)) == 1

    def test_status_ok_no_state_on_spanner(self, monkeypatch):
        # Given
        spanner_called = _mock_spanner_module(monkeypatch)
        # When
        result = self.obj.status
        # Then
        assert result == mutex.MutexStatus.UNKNOWN
        assert len(spanner_called.get(spanner_mutex.spanner.read_table_rows.__name__)) == 1

    def test_start_ok_does_not_need_mutex(self, monkeypatch):
        # Given
        self.obj.is_mutex_needed_value = False
        spanner_called = _mock_spanner_module(monkeypatch)
        # When
        self.obj.start()
        # Then
        _validate_start_calls(spanner_called, self.obj)

    @pytest.mark.parametrize(
        "kwargs_mock",
        [
            dict(  # empty state -> can_upsert is True
                read_table_rows=None,
                can_upsert_args=[
                    None,
                    None,
                    _mutex_state(status=mutex.MutexStatus.STARTED).to_spanner_row(with_commit_ts=False),
                ],
            ),
            dict(  # state breaches jitter and state *not* STARTED -> can_upsert is True
                read_table_rows=[
                    _mutex_state(
                        breaches_jitter=True,
                        status=mutex.MutexStatus.FAILED,
                    ).to_spanner_row(with_commit_ts=False)
                ],
            ),
            dict(  # state breaches watermark -> can_upsert is True
                read_table_rows=[
                    _mutex_state(
                        breach_watermark=True,
                        status=mutex.MutexStatus.STARTED,
                    ).to_spanner_row(with_commit_ts=False)
                ],
            ),
            dict(  # different client but state is stale -> can_upsert is True
                read_table_rows=[
                    _mutex_state(
                        is_stale=True,
                        update_client_uuid=uuid.uuid4(),
                        status=mutex.MutexStatus.DONE,
                    ).to_spanner_row(with_commit_ts=False)
                ],
            ),
        ],
    )
    def test_start_ok_needs_mutex_and_gets_mutex(self, monkeypatch, kwargs_mock: Dict[str, Any]):
        # Given
        self.obj.is_mutex_needed_value = True
        spanner_called = _mock_spanner_module(monkeypatch, **kwargs_mock)
        # When
        self.obj.start()
        # Then
        _validate_start_calls(spanner_called, self.obj)

    @pytest.mark.parametrize(
        "kwargs_mock",
        [
            dict(  # state DONE and *not* stale
                read_table_rows=[
                    _mutex_state(
                        breach_watermark=True,
                        status=mutex.MutexStatus.DONE,
                    ).to_spanner_row(with_commit_ts=False)
                ],
            ),
            dict(  # state DONE by different client and *not* stale
                read_table_rows=[
                    _mutex_state(
                        breach_watermark=True,
                        update_client_uuid=uuid.uuid4(),
                        status=mutex.MutexStatus.DONE,
                    ).to_spanner_row(with_commit_ts=False)
                ],
            ),
        ],
    )
    def test_start_ok_needs_mutex_and_does_not_get_mutex(self, monkeypatch, kwargs_mock: Dict[str, Any]):
        # Given
        self.obj.is_mutex_needed_value = True
        spanner_called = _mock_spanner_module(monkeypatch, **kwargs_mock)
        # When
        self.obj.start()
        # Then
        is_mutex_needed = self.obj.called.get(self.obj.__class__.is_mutex_needed.__name__)
        assert len(is_mutex_needed) == self.obj.config.mutex_max_retries
        execute_critical_section = self.obj.called.get(self.obj.__class__.execute_critical_section.__name__)
        assert len(execute_critical_section) == 0


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
    read_table_rows = read_table_rows if read_table_rows else []
    read_in_transaction = read_in_transaction if read_in_transaction else read_table_rows

    def _add_to_result(key: str, value: Any) -> None:
        nonlocal result
        where = result.get(key)
        if where is None:
            where = []
            result[key] = where
        where.append(value)

    def mocked_read_table_rows(*args, **kwargs) -> Any:
        _add_to_result(spanner_mutex.spanner.read_table_rows.__name__, locals())
        return iter(read_table_rows)

    def mocked_read_in_transaction(*args, **kwargs) -> Any:
        _add_to_result(spanner_mutex.spanner.read_in_transaction.__name__, locals())
        return iter(read_in_transaction)

    def mocked_conditional_upsert_table_row(*args, **kwargs) -> Any:
        _add_to_result(spanner_mutex.spanner.conditional_upsert_table_row.__name__, locals())
        res = conditional_upsert_table_row
        if "can_upsert" in kwargs:
            nonlocal can_upsert_args
            can_upsert_args = can_upsert_args if can_upsert_args else [None, None, kwargs.get("row")]
            res = kwargs.get("can_upsert")(*can_upsert_args)
        return res

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


def _validate_start_calls(spanner_called: Dict[str, List[Any]], obj: MySpannerMutex, executes: bool = True) -> None:
    # Then: is_mutex_needed
    is_mutex_needed = obj.called.get(obj.__class__.is_mutex_needed.__name__)
    assert len(is_mutex_needed) == 1
    # Then: spanner_called: spanner_db
    called_spanner_db = spanner_called.get(spanner_mutex.spanner.spanner_db.__name__)
    # cache tools is tricky
    assert called_spanner_db is None or len(called_spanner_db) == 1
    # Then: spanner_called: spanner_table
    called_spanner_table = spanner_called.get(spanner_mutex.spanner.spanner_table.__name__)
    assert len(called_spanner_table) == 1
    # Then: execute_critical_section
    execute_critical_section = obj.called.get(obj.__class__.execute_critical_section.__name__)
    if not obj.is_mutex_needed_value:
        assert len(execute_critical_section) == 0
    else:
        assert len(execute_critical_section) == 1
        # Then: spanner_called: read_table_rows
        called_read_table_rows = spanner_called.get(spanner_mutex.spanner.read_table_rows.__name__)
        assert len(called_read_table_rows) == 1
        kwargs_read_table_rows = called_read_table_rows[0].get("kwargs", {})
        assert kwargs_read_table_rows.get("table_id") == obj.config.table_id
        kwargs_keys_read_table_rows = list(kwargs_read_table_rows.get("keys"))
        assert len(kwargs_keys_read_table_rows) == 1
        assert kwargs_keys_read_table_rows[0][0] == str(obj.config.mutex_uuid)
        # Then: spanner_called: conditional_upsert_table_row
        called_conditional_upsert_table_row = spanner_called.get(
            spanner_mutex.spanner.conditional_upsert_table_row.__name__
        )
        assert len(called_conditional_upsert_table_row) == 2  # one to set and one to release the mutex
        for el in called_conditional_upsert_table_row:
            kwargs = el.get("kwargs", {})
            assert kwargs.get("table_id") == obj.config.table_id
            kwargs_row = kwargs.get("row", {})
            assert kwargs_row.get("uuid") == str(obj.config.mutex_uuid)
            assert kwargs_row.get("update_client_uuid") == str(obj.client_uuid)
            assert kwargs_row.get("update_client_display_name") == obj.client_display_name
        # Then: spanner_called: read_in_transaction
        called_read_in_transaction = spanner_called.get(spanner_mutex.spanner.read_in_transaction.__name__)
        assert len(called_read_in_transaction) == 2  # one to set and one to release the mutex
        for el in called_read_in_transaction:
            kwargs_keys = el.get("kwargs", {}).get("keys")
            assert len(kwargs_keys) == 1
            assert list(kwargs_keys)[0][0] == str(obj.config.mutex_uuid)
