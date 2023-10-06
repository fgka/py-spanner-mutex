# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Using Spanner creates a distributed critical section (mutex).
"""
import abc
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional

import cachetools
import pytz
from google.auth import credentials  # type: ignore
from google.cloud.spanner_v1 import database, table, transaction

from py_spanner_mutex.common import logger, preprocess
from py_spanner_mutex.dto import mutex
from py_spanner_mutex.gcp import spanner

_SPANNER_DATABASE_CACHE_TTL_IN_SECONDS: timedelta = timedelta(minutes=30)
_MUTEX_TTL_JITTER_IN_PERCENT: float = 0.05  # 5%
_RAND: random.Random = random.Random()  # for testing purposes, mock the method using it
_LOGGER = logger.get(__name__)


class SpannerMutexError(RuntimeError):
    """
    Aggregates all errors in :py:class:`SpannerMutex`.
    """


class SpannerMutex(abc.ABC):
    """
    Mutex base class that needs to be extended.

    **WARNING**: This **can** be thread-safe iff ``client_uuid`` is unique for each thread.
    If you do **not** provide the ``client_uuid`` it will create on per instance, making it thread-safe.
    In this case we do recommend that you set ``client_display_name`` in a meaningful way that lets you identify the
    corresponding process/thread.
    """

    def __init__(
        self,
        *,
        config: mutex.MutexConfig,
        client_uuid: Optional[uuid.UUID] = None,
        client_display_name: Optional[str] = None,
        creds: Optional[credentials.Credentials] = None,
    ):
        _LOGGER.debug("Creating '%s' with '%s'", self.__class__.__name__, locals())
        self._config: mutex.MutexConfig = preprocess.validate_type(config, "config", mutex.MutexConfig)  # type: ignore
        self._client_uuid: uuid.UUID = preprocess.validate_type(  # type: ignore
            client_uuid, "client_uuid", uuid.UUID, is_none_valid=True, default_value=uuid.uuid4()
        )
        self._client_display_name: str = preprocess.string(  # type: ignore
            client_display_name, "client_display_name", is_none_valid=True, default_value=str(self._client_uuid)
        )
        self._creds = preprocess.validate_type(creds, "creds", credentials.Credentials, is_none_valid=True)

    @property
    def config(self) -> mutex.MutexConfig:
        """
        Config object
        """
        return self._config

    @property
    def client_uuid(self) -> uuid.UUID:
        """
        This client's UUID
        """
        return self._client_uuid

    @property
    def client_display_name(self) -> str:
        """
        This client's display name
        """
        return self._client_display_name

    def validate(self, raise_if_invalid: Optional[bool] = True) -> bool:
        """
        Will check if the full Spanner infrastructure is ready, i.e.:
        * Instance exists
        * Database exists and is ready
        * Table exists

        Args:
            raise_if_invalid:

        Returns:
        Raises:
            SpannerMutexError: in all runtime errors
        """
        tbl = None
        try:
            tbl = _spanner_table(
                instance_id=self._config.instance_id,
                database_id=self._config.database_id,
                project_id=self._config.project_id,
                creds=self._creds,
                table_id=self._config.table_id,
                must_exist=True,
            )
        except Exception as err:
            msg = f"Could not retrieve Spanner database. Object: {str(self)}. Error: {err}"
            if raise_if_invalid:
                raise SpannerMutexError(msg) from err
            else:
                _LOGGER.error(msg)
        return tbl is not None

    @property
    def status(self) -> mutex.MutexStatus:
        """
        Current mutex status
        """
        result = mutex.MutexStatus.UNKNOWN
        state = self._state()
        if state is not None:
            result = state.status
        return result

    def _state(self) -> Optional[mutex.MutexState]:
        result = None
        try:
            raw_row = next(
                spanner.read_table_rows(
                    db=self._mutex_db(), table_id=self._config.table_id, keys={(str(self._config.mutex_uuid),)}
                )
            )
            result = mutex.MutexState.from_spanner_row(raw_row)
        except StopIteration:
            _LOGGER.debug("There is no mutex state entry for mutex ID '%s'", self._config.mutex_uuid)
        return result

    def _mutex_db(self) -> database.Database:
        return _spanner_db(
            instance_id=self._config.instance_id,
            database_id=self._config.database_id,
            project_id=self._config.project_id,
            creds=self._creds,
        )

    @abc.abstractmethod
    def is_mutex_needed(self) -> bool:
        """
        It should check if the mutex is still needed or not. Usually you have two categories of mutexes:
        * Mutex is always needed, in this case this method is the same as checking if the work has been done.
        * Mutex need depends on an external factor and sometimes there are no tasks/jobs.
            In this case you should check if there are tasks/jobs to be executed in the critical section.
        """

    @abc.abstractmethod
    def execute_critical_section(self, max_end_time: datetime) -> None:
        """
        This method will be called if there is a task/job to be executed in the critical section.
        **AND** this client/thread has the lock on the mutex.

        Args:
            max_end_time: this is your time budget, once this time is reached you should expect that
                another client/thread will try to execute the critical section, assuming you couldn't.
        """

    def start(self) -> None:
        """
        Will start the critical section.
        """
        # can start?
        self.validate(raise_if_invalid=True)
        retries = 0
        start_time = time.time()
        has_executed = False
        while self._safe_is_mutex_needed() and retries < self._config.mutex_max_retries:
            _LOGGER.debug("Critical section is needed at: %s", self)
            state = self._state()
            _LOGGER.debug("Current mutex state '%s'", state)
            if self._should_try_to_acquire_mutex(state):
                if self._acquire_mutex():
                    try:
                        _LOGGER.info("Mutex acquired, executing critical section for: %s", str(self))
                        self._safe_execute_critical_section(self._max_end_time())
                        self._release_mutex()
                        _LOGGER.info("Critical section executed and mutex released successfully for: %s", str(self))
                        has_executed = True
                        break
                    except Exception as err:
                        _LOGGER.critical("Failed critical section at: '%s'. Error: %s", str(self), err)
                        self._release_mutex(error=err)
            _LOGGER.debug(
                "Waiting '%d' seconds for next mutex check cycle on '%s'",
                self._config.mutex_wait_time_in_secs,
                str(self),
            )
            time.sleep(self._config.mutex_wait_time_in_secs)
            retries += 1
        # end by logging
        elapsed_time = time.time() - start_time
        if retries > self._config.mutex_max_retries and not has_executed:
            _LOGGER.critical("Max retries %d reached after %f seconds. Client: %s", retries, elapsed_time, str(self))
        else:
            _LOGGER.info(
                "Critical section execution = %r and ended after %d retries and %f seconds. Client: %s",
                has_executed,
                retries,
                elapsed_time,
                str(self),
            )

    def _safe_is_mutex_needed(self) -> bool:
        method_name = f"{self.__class__.__name__}.{self.__class__.is_mutex_needed.__name__}"
        return self._safe_method_execution(method_name, lambda: self.is_mutex_needed())

    def _safe_method_execution(self, method_name: str, func: Callable) -> Any:
        start_time = time.time()
        try:
            result = func()
        except Exception as err:
            raise SpannerMutexError(f"Could not execute '{method_name}'. Error: {err}") from err
        elapsed_time = time.time() - start_time
        _LOGGER.debug(f"Executed '{method_name}' in {elapsed_time} seconds. Client: {self}")
        return result

    def _should_try_to_acquire_mutex(self, state: Optional[mutex.MutexState]) -> bool:
        """
        This client should try to acquire the mutex in one of the following cases:
        * ``state`` is :py:obj:`None` (no other client tried until now)
        * ``state`` is stale
        * the status is **not** done **AND** the current client exceeded the TTL.
        """
        result = False
        # 'if's used for improved readability
        if state is None:
            result = True
        elif self._is_state_stale(state):
            result = True
        elif not self._is_critical_section_done_or_started(state) and self._is_watermark_breached(state):
            result = True
        return result

    def _is_state_stale(self, state: Optional[mutex.MutexState]) -> bool:
        """
        Either the state is :py:obj:`None` or the last update is older than mutex staleness threshold.
        """
        return state is None or (
            state.update_time_utc.replace(tzinfo=pytz.UTC) + timedelta(seconds=self._config.mutex_staleness_in_secs)
            < datetime.utcnow().replace(tzinfo=pytz.UTC)
        )

    @staticmethod
    def _is_critical_section_done_or_started(state: Optional[mutex.MutexState]) -> bool:
        return state is not None and state.status in (mutex.MutexStatus.DONE, mutex.MutexStatus.STARTED)

    def _is_watermark_breached(self, state: Optional[mutex.MutexState]) -> bool:
        result = True
        if state is not None:
            jitter_ttl_in_secs = self._config.mutex_ttl_in_secs + self._jitter_in_secs()
            result = state.is_state_stale(jitter_ttl_in_secs)
        return result

    def _jitter_in_secs(self) -> int:
        max_jitter_in_secs = int(max(self._config.mutex_ttl_in_secs * _MUTEX_TTL_JITTER_IN_PERCENT, 1))
        return _RAND.randint(0, max_jitter_in_secs)

    def _safe_execute_critical_section(self, max_end_time: datetime) -> None:
        method_name = f"{self.__class__.__name__}.{self.__class__.execute_critical_section.__name__}"
        return self._safe_method_execution(
            method_name, lambda: self.execute_critical_section(max_end_time=max_end_time)
        )

    def _acquire_mutex(self) -> bool:
        state = self._create_state(mutex.MutexStatus.STARTED)
        return self._set_mutex(state)

    def _create_state(self, status: mutex.MutexStatus) -> mutex.MutexState:
        display_name = (
            self._config.mutex_display_name
            if self._config.mutex_display_name is not None
            else str(self._config.mutex_uuid)
        )
        return mutex.MutexState(
            uuid=self._config.mutex_uuid,
            display_name=display_name,
            status=status,
            update_time_utc=datetime.utcnow(),
            update_client_uuid=self._client_uuid,
            update_client_display_name=self._client_display_name,
        )

    def _set_mutex(self, state: mutex.MutexState) -> bool:
        """
        **IMPORTANT** This code is not strictly correct and there is a non-zero chance it will fail.
        For all the details, please read the code/CORRECTNESS_DISCLAIMER.md
        Args:
            state:

        Returns:

        """

        def can_upsert(txn: transaction.Transaction, tbl: table.Table, row: Dict[str, Any]) -> bool:
            _LOGGER.debug("Checking if upsert should be performed, args: %s", locals())
            res = True
            try:
                existing_row = next(
                    spanner.read_in_transaction(txn=txn, tbl=tbl, keys={(str(self._config.mutex_uuid),)})
                )
                curr_state = mutex.MutexState.from_spanner_row(existing_row)
                if curr_state.update_client_uuid == self._client_uuid and row.get("status") != existing_row.get(
                    "status"
                ):
                    # here it is the same client updating the mutex and changing the status,
                    # for instance from STARTED to DONE
                    res = True
                else:
                    res = self._should_try_to_acquire_mutex(curr_state)
                    _LOGGER.debug("Current mutex state '%s' and result on can upsert = %r", curr_state, res)
            except StopIteration:
                _LOGGER.debug("There is no mutex state entry for mutex ID '%s'", self._config.mutex_uuid)
            return res

        row = state.to_spanner_row()
        try:
            result = spanner.conditional_upsert_table_row(
                db=self._mutex_db(), table_id=self._config.table_id, row=row, can_upsert=can_upsert
            )
        except Exception as err:
            _LOGGER.info("Could not set mutex to '%s' at '%s'. Row: '%s'. Error: %s", state, str(self), row, err)
            result = False
        return result

    def _max_end_time(self) -> datetime:
        return datetime.utcnow().replace(tzinfo=pytz.UTC) + timedelta(seconds=self._config.mutex_ttl_in_secs)

    def _release_mutex(self, error: Optional[Exception] = None) -> None:
        status = mutex.MutexStatus.DONE if error is None else mutex.MutexStatus.FAILED
        state = self._create_state(status)
        if not self._set_mutex(state):
            raise SpannerMutexError(
                f"Could not release mutex using state '{state}' at '{self}'. "
                f"Error executing critical section: '{error}'. "
                "See logs for errors releasing the mutex."
            )
        if error:
            raise SpannerMutexError(f"Released mutex but execution of critical section failed at '{self}'") from error

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"config='{self._config}', "
            f"client_uuid='{self._client_uuid}', "
            f"client_display_name='{self._client_display_name}', "
            f"creds='{self._creds}')"
        )


def _spanner_table(
    *,
    instance_id: str,
    database_id: str,
    project_id: Optional[str] = None,
    creds: Optional[credentials.Credentials] = None,
    table_id: str,
    must_exist: Optional[bool] = True,
) -> table.Table:
    db = _spanner_db(instance_id=instance_id, database_id=database_id, project_id=project_id, creds=creds)
    return spanner.spanner_table(db=db, table_id=table_id, must_exist=must_exist)


@cachetools.cached(cachetools.TTLCache(maxsize=10, ttl=_SPANNER_DATABASE_CACHE_TTL_IN_SECONDS.total_seconds()))
def _spanner_db(
    *,
    instance_id: str,
    database_id: str,
    project_id: Optional[str] = None,
    creds: Optional[credentials.Credentials] = None,
) -> database.Database:
    """
    Memoized version of :py:func:`spanner.spanner_db`
    Args:
        instance_id:
        database_id:
        project_id:
        creds:

    Returns:

    """
    return spanner.spanner_db(instance_id=instance_id, database_id=database_id, project_id=project_id, creds=creds)
