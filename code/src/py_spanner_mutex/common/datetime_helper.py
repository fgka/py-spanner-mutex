# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Wrapping :py:mod:`datetime` to enforce UTC and to allow testing to mock it.
"""
from datetime import datetime, timedelta
from typing import Optional

import pytz

from py_spanner_mutex.common import preprocess


def datetime_utcnow_with_tzinfo(*, delta_in_secs: Optional[int] = None) -> datetime:
    """
    Returns ``datetime.utcnow()`` with timezone info set at :py:data:`pytz.UTC`.
    Args:
        delta_in_secs: adds that many seconds to _now_, if given.

    Returns:

    See Also:
        :py:func:`datetime_with_utc_tzinfo`
    """
    return datetime_with_utc_tzinfo(_datetime_utcnow(), delta_in_secs=delta_in_secs)


def _datetime_utcnow() -> datetime:
    # For testing
    return datetime.utcnow()


def datetime_with_utc_tzinfo(value: datetime, *, delta_in_secs: Optional[int] = None) -> datetime:
    """
    Returns ``value`` with timezone info set at :py:data:`pytz.UTC`.
    Uses :py:meth:`datetime.replace` to add timezone information.

    Args:
        value: :py:cls:`datetime` to have :py:data:`pytz.UTC` add to it
        delta_in_secs: adds that many seconds to ``value``, if given.

    Returns:

    See Also:
        :py:func:`datetime_utcnow_with_tzinfo`
    """
    preprocess.validate_type(value, "value", datetime)
    delta_in_secs = preprocess.integer(delta_in_secs, "delta_in_secs", is_none_valid=True)
    result = value.replace(tzinfo=pytz.UTC)
    if delta_in_secs is not None:
        result += timedelta(seconds=delta_in_secs)
    return result
