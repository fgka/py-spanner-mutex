# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""Mutex table row"""
import uuid as sys_uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import attrs
import pytz
from google.cloud import spanner

from py_spanner_mutex.common import const, dto_defaults


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


@attrs.define(**const.ATTRS_DEFAULTS)
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
        return self.update_time_utc + timedelta(seconds=ttl_in_secs) > datetime.utcnow()

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
            uuid=sys_uuid.UUID(f"{{{row.get('uuid')}}}"),
            display_name=row.get("display_name"),
            status=MutexStatus.from_str(row.get("status")),
            update_time_utc=update_time_utc,
            update_client_uuid=sys_uuid.UUID(f"{{{row.get('update_client_uuid')}}}"),
            update_client_display_name=row.get("update_client_display_name"),
        )

    def to_spanner_row(self, with_commit_ts: Optional[bool] = True) -> Dict[str, Any]:
        result = self.as_dict()
        result["uuid"] = str(self.uuid)
        result["update_client_uuid"] = str(self.update_client_uuid)
        if with_commit_ts:
            result["update_time_utc"] = spanner.COMMIT_TIMESTAMP
        return result
