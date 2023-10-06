# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""Mutex table row"""
import uuid as sys_uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

import attrs
import pytz
from google.cloud import spanner  # type: ignore

from py_spanner_mutex.common import const, dto_defaults

MIN_MUTEX_TTL_IN_SECONDS: int = 10  # 10 second
DEFAULT_MUTEX_TTL_IN_SECONDS: int = 5 * 60  # 5 minutes
MIN_MUTEX_WAIT_TIME_IN_SECONDS: int = 1  # 1 second
DEFAULT_MUTEX_WAIT_TIME_IN_SECONDS: int = 10  # 10 seconds
MIN_MUTEX_STALENESS_IN_SECONDS: int = MIN_MUTEX_TTL_IN_SECONDS + 1  # must be greater than TTL
DEFAULT_MUTEX_STALENESS_IN_SECONDS: int = 2 * DEFAULT_MUTEX_TTL_IN_SECONDS
MIN_MUTEX_MAX_RETRIES: int = 5
DEFAULT_MUTEX_MAX_RETRIES: int = 50


class MutexStatus(dto_defaults.EnumWithFromStrIgnoreCase):
    """Supported mutex status."""

    DONE = "done"
    STARTED = "started"
    FAILED = "failed"
    UNKNOWN = ""

    @classmethod
    def default(cls) -> Any:
        """Default caching type."""
        return MutexStatus.UNKNOWN


@attrs.define(**const.ATTRS_DEFAULTS)  # type: ignore
class MutexState(dto_defaults.HasFromJsonString):
    """This must be in sync with the table schema."""

    uuid: sys_uuid.UUID = attrs.field(validator=attrs.validators.instance_of(sys_uuid.UUID))
    display_name: str = attrs.field(validator=attrs.validators.instance_of(str))
    status: MutexStatus = attrs.field(validator=attrs.validators.instance_of(MutexStatus))
    update_time_utc: datetime = attrs.field(validator=attrs.validators.instance_of(datetime))
    update_client_uuid: sys_uuid.UUID = attrs.field(validator=attrs.validators.instance_of(sys_uuid.UUID))
    update_client_display_name: str = attrs.field(validator=attrs.validators.instance_of(str))

    def is_state_stale(self, ttl_in_secs: int) -> bool:
        """
        Will verify if the ``update_time`` is older than ``ttl_in_secs``
        Args:
            ttl_in_secs:
        Returns:

        """
        return self.update_time_utc.replace(tzinfo=pytz.UTC) + timedelta(
            seconds=ttl_in_secs
        ) > datetime.utcnow().replace(tzinfo=pytz.UTC)

    @staticmethod
    def from_spanner_row(row: Dict[str, Any]) -> "MutexState":
        """
        This must be where columns are mapped to fields. **MUST** be in sync with table schema in Spanner.
        Args:
            row:

        Returns:

        """
        update_time_utc = row.get("update_time_utc", datetime.utcnow()).astimezone(pytz.UTC)
        return MutexState(
            uuid=_str_uuid_converter(row.get("uuid")),  # type: ignore
            display_name=row.get("display_name"),  # type: ignore
            status=MutexStatus.from_str(row.get("status")),
            update_time_utc=update_time_utc,
            update_client_uuid=sys_uuid.UUID(f"{{{row.get('update_client_uuid')}}}"),
            update_client_display_name=row.get("update_client_display_name"),  # type: ignore
        )

    def to_spanner_row(self, with_commit_ts: Optional[bool] = True) -> Dict[str, Any]:
        result = self.as_dict()
        result["uuid"] = str(self.uuid)
        result["status"] = self.status.value
        result["update_client_uuid"] = str(self.update_client_uuid)
        if with_commit_ts:
            result["update_time_utc"] = spanner.COMMIT_TIMESTAMP
        return result


def _str_uuid_converter(value: Union[sys_uuid.UUID, str]) -> sys_uuid.UUID:
    if isinstance(value, sys_uuid.UUID):
        result = value
    elif isinstance(value, str):
        result = sys_uuid.UUID(f"{{{value}}}")
    else:
        raise ValueError(f"Value '{value}'({type(value)}) is not supported")
    return result


@attrs.define(**const.ATTRS_DEFAULTS)  # type: ignore
class MutexConfig(dto_defaults.HasFromJsonString):
    """Configuration DTO."""

    mutex_uuid: sys_uuid.UUID = attrs.field(
        converter=_str_uuid_converter, validator=attrs.validators.instance_of(sys_uuid.UUID)
    )
    """
    The key to the mutex, each mutex should have its own UUID.
    It must be the same for all clients.
    """
    instance_id: str = attrs.field(validator=attrs.validators.instance_of(str))
    """
    Spanner instance ID
    """
    database_id: str = attrs.field(validator=attrs.validators.instance_of(str))
    """
    Spanner database ID, hosted by the instance
    """
    table_id: str = attrs.field(validator=attrs.validators.instance_of(str))
    """
    Spanner table name that holds all mutexes, it belongs to the database given.
    """
    project_id: Optional[str] = attrs.field(
        default=None, validator=attrs.validators.optional(attrs.validators.instance_of(str))
    )
    """
    Google cloud project, if not the default where the clients are running.
    """
    mutex_display_name: Optional[str] = attrs.field(
        default=None, validator=attrs.validators.optional(attrs.validators.instance_of(str))
    )
    """
    There is no functional need, just to make displaying and debugging easier.
    """
    mutex_ttl_in_secs: int = attrs.field(
        default=DEFAULT_MUTEX_TTL_IN_SECONDS,
        validator=attrs.validators.and_(  # type: ignore
            attrs.validators.optional(attrs.validators.instance_of(int)), attrs.validators.ge(MIN_MUTEX_TTL_IN_SECONDS)  # type: ignore
        ),
    )
    """
    The TTL is allotted time given to the client that acquire the critical section to execute it.
    If this time is exceeded, other clients will assume it fails and will try to acquire and execute
    the critical section.
    """
    mutex_staleness_in_secs: int = attrs.field(
        default=DEFAULT_MUTEX_STALENESS_IN_SECONDS,
        validator=attrs.validators.and_(  # type: ignore
            attrs.validators.optional(attrs.validators.instance_of(int)),  # type: ignore
            attrs.validators.ge(MIN_MUTEX_STALENESS_IN_SECONDS),
        ),
    )
    """
    If a "DONE" status is found but older than the given staleness,
    it will assume it is from a past execution and ignore it.
    """
    mutex_wait_time_in_secs: int = attrs.field(
        default=DEFAULT_MUTEX_WAIT_TIME_IN_SECONDS,
        validator=attrs.validators.and_(  # type: ignore
            attrs.validators.optional(attrs.validators.instance_of(int)),  # type: ignore
            attrs.validators.ge(MIN_MUTEX_WAIT_TIME_IN_SECONDS),
        ),
    )
    """
    If the client cannot get the critical section, it will use the wait_time before retrying.
    """
    mutex_max_retries: int = attrs.field(
        default=DEFAULT_MUTEX_MAX_RETRIES,
        validator=attrs.validators.and_(  # type: ignore
            attrs.validators.optional(attrs.validators.instance_of(int)),  # type: ignore
            attrs.validators.ge(MIN_MUTEX_MAX_RETRIES),
        ),
    )
    """
    After going max_retries trying the acquire the critical section, will fail and give up.
    """

    def __attrs_post_init__(self):
        """
        Staleness *MUST* be higher that all retries and TTL.
        """
        max_retries_time = self.mutex_max_retries * self.mutex_wait_time_in_secs
        max_mutex_active_time = max(max_retries_time, self.mutex_ttl_in_secs)
        if self.mutex_staleness_in_secs <= max_mutex_active_time:
            raise ValueError(
                f"Staleness value '{self.mutex_staleness_in_secs}' *MUST* be higher (preferably considerably so) than "
                f"the maximum of TTL ({self.mutex_ttl_in_secs}) and retry max time ({max_retries_time} = "
                f"retries ({self.mutex_max_retries}) * wait time {self.mutex_wait_time_in_secs})). "
                f"All values: {self}"
            )
