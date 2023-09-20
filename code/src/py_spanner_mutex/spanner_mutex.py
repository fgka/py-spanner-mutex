# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Using Spanner creates a distributed critical section (mutex).
"""
import abc
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import cachetools
from google.auth import credentials
from google.cloud.spanner_v1 import database, table

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
        self._config = preprocess.validate_type(config, "config", mutex.MutexConfig)
        self._client_uuid = preprocess.validate_type(
            client_uuid, "client_uuid", uuid.UUID, is_none_valid=True, default_value=uuid.uuid4()
        )
        self._client_display_name = preprocess.string(
            client_display_name, "client_display_name", is_none_valid=True, default_value=str(self._client_uuid)
        )
        self._creds = preprocess.validate_type(creds, "creds", credentials.Credentials, is_none_valid=True)

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
        return self._state().status

    def _state(self) -> Optional[mutex.MutexState]:
        raw_row = next(
            spanner.read_table_rows(
                db=self._mutex_db(), table_id=self._config.table_id, keys={str(self._config.mutex_uuid)}
            )
        )
        return mutex.MutexState.from_spanner_row(raw_row)

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
        while self.is_mutex_needed():
            _LOGGER.info("Critical section is needed at: %s", self)
            state = self._state()
            _LOGGER.debug("Current mutex state '%s'", state)
            if self._should_try_to_acquire_mutex(state):
                if self._acquire_mutex():
                    try:
                        _LOGGER.info("Mutex acquired, executing critical section for: %s", str(self))
                        self.execute_critical_section(self._max_end_time())
                        self._release_mutex()
                        _LOGGER.info("Critical section executed and mutex released successfully for: %s", str(self))
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
        elif not self._is_critical_section_done(state) and self._is_watermark_breached(state):
            result = True
        return result

    def _is_state_stale(self, state: Optional[mutex.MutexState]) -> bool:
        """
        Either the state is :py:obj:`None` or the last update is older than mutex staleness threshold.
        """
        return state is None or (
            state.update_time_utc + timedelta(seconds=self._config.mutex_staleness_in_secs) < datetime.utcnow()
        )

    @staticmethod
    def _is_critical_section_done(state: Optional[mutex.MutexState]) -> bool:
        return state is not None and state.status == mutex.MutexStatus.DONE

    def _is_watermark_breached(self, state: Optional[mutex.MutexState]) -> bool:
        result = True
        if state is not None:
            jitter_ttl_in_secs = self._config.mutex_ttl_in_secs + self._jitter_in_secs()
            result = state.is_state_stale(jitter_ttl_in_secs)
        return result

    def _jitter_in_secs(self) -> int:
        max_jitter_in_secs = int(max(self._config.mutex_ttl_in_secs * _MUTEX_TTL_JITTER_IN_PERCENT, 1))
        return _RAND.randint(0, max_jitter_in_secs)

    def _acquire_mutex(self) -> bool:
        state = self._create_state(mutex.MutexStatus.STARTED)
        return self._set_mutex(state)

    def _create_state(self, status: mutex.MutexStatus) -> mutex.MutexState:
        return mutex.MutexState(
            uuid=self._config.mutex_uuid,
            display_name=self._config.mutex_display_name,
            status=status,
            update_time_utc=datetime.utcnow(),
            update_client_uuid=self._client_uuid,
            update_client_display_name=self._client_display_name,
        )

    def _set_mutex(self, state: mutex.MutexState) -> bool:
        try:
            spanner.upsert_table_row(db=self._mutex_db(), table_id=self._config.table_id, row=state.to_spanner_row())
            result = True
        except Exception as err:
            _LOGGER.info("Could not set mutex to '%s' at '%s'. Error: %s", state, str(self), err)
            result = False
        return result

    def _max_end_time(self) -> datetime:
        return datetime.utcnow() + timedelta(seconds=self._config.mutex_ttl_in_secs)

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


@cachetools.cached(cachetools.TTLCache(maxsize=10, ttl=_SPANNER_DATABASE_CACHE_TTL_IN_SECONDS))
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
