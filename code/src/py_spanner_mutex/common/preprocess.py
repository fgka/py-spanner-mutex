# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""Common value pre-processing and validation."""
import os
import pathlib
import re
from typing import Any, Callable, Optional, Union

_DEFAULT_TYPE_VALUE: type = object
_DEFAULT_NAME_VALUE: str = "value"


def validate_type(
    value: Any,
    name: str = _DEFAULT_NAME_VALUE,
    cls: type = _DEFAULT_TYPE_VALUE,
    *,
    is_none_valid: bool = False,
    default_value: Optional[Any] = None,
) -> Optional[Any]:
    """
    Verifies if a particular ``value`` is of the correct type.

    Args:
        value: what to test.
        name: name of the argument (for error reporting). Default: ``"value"``.
        cls: which :py:class:`type` to test against. Default: py:class:`object`
        is_none_valid: accept :py:obj:`None` as valid. Default: py:obj:`False`.
        default_value: what to return if the argument is :py:obj:`None`. Default: :py:obj:`None`.
    Returns:

    """
    result = value
    if result is None:
        result = default_value
    if not isinstance(result, cls):
        err = TypeError(
            f"Value of '{name}' must be a {cls.__name__}. Got: '{value}'({type(value)}) / '{result}'({type(result)})"
        )
        if result is None:
            if not is_none_valid:
                raise err
        else:
            raise err
    return result


def string(
    value: Any,
    name: str = _DEFAULT_NAME_VALUE,
    *,
    regex: Optional[Union[str | re.Pattern]] = None,
    strip_it: bool = True,
    is_empty_valid: bool = False,
    is_none_valid: bool = False,
    default_value: Optional[Any] = None,
) -> Optional[str]:
    """
    Returns validated ``value`` argument. Behavior, in order:
    1. Check if is an instance of :py:class:`str`.
      * If it is not a :py:class:`str`, check if it is :py:obj:`None`
        and that it is acceptable (by default it does not).
    1. (By default) strip the content.
    1. Check if content is empty and if that is permitted (by default it is not).

    Args:
        value: what to test.
        name: name of the argument (for error reporting). Default: ``"value"``.
        regex: if given, will validate the string against it. Default: :py:objt:`None`.
        strip_it: to strip input value. Default: py:obj:`True`.
        is_empty_valid: accept empty strings as valid. Default: py:obj:`False`.
        is_none_valid: accept :py:obj:`None` as valid. Default: py:obj:`False`.
        default_value: what to return if the argument is :py:obj:`None`. Default: :py:obj:`None`.

    Returns:
        Validated input ``value``.
    """
    # logic
    result = validate_type(value=value, name=name, cls=str, is_none_valid=is_none_valid, default_value=default_value)
    if isinstance(result, str):
        if strip_it:
            result = result.strip()
        if not result and not is_empty_valid:
            raise ValueError(
                f"Value of '{name}' must be a non-empty {str.__name__}. "
                f"Strip before checking = {strip_it}. "
                f"Got: '{value}'({type(value)}) / '{result}'({type(result)})"
            )
        if regex is not None:
            if isinstance(regex, str):
                regex = re.compile(regex)
            elif not isinstance(regex, re.Pattern):
                raise TypeError(
                    f"The validation argument 'regex' must be either a '{str.__name__}' or '{re.Pattern.__name__}'. "
                    f"Got: '{regex}'({type(regex)})"
                )
            match = regex.match(value)
            if not match:
                raise ValueError(
                    f"Argument '{name}' must be a '{str.__name__}' conforming the regular expression '{regex}'. "
                    f"Got: '{value}'"
                )
    return result


def integer(
    value: Any,
    name: str = _DEFAULT_NAME_VALUE,
    *,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    is_none_valid: bool = False,
    default_value: Optional[Any] = None,
) -> Optional[int]:
    """

    Args:
        value: what to test.
        name: name of the argument (for error reporting). Default: ``"value"``.
        lower_bound: minimum acceptable value. Default: py:obj:`None`.
        upper_bound: maximum acceptable value. Default: py:obj:`None`.
        is_none_valid: accept :py:obj:`None` as valid. Default: py:obj:`False`.
        default_value: what to return if the argument is :py:obj:`None`. Default: :py:obj:`None`.

    Returns:

    """
    # validate input
    if lower_bound is not None and upper_bound is not None and lower_bound > upper_bound:
        raise RuntimeError(
            f"Validation arguments for limits must obey 'lower_bound < upper_bound'. "
            f"Got: [{lower_bound}, {upper_bound}]"
        )
    result = validate_type(value=value, name=name, cls=int, is_none_valid=is_none_valid, default_value=default_value)
    if isinstance(result, int):
        if lower_bound is not None and result < lower_bound:
            raise ValueError(
                f"Argument '{name}' needs to be an {int.__name__} >= {lower_bound}. Got: '{result}'({type(result)})"
            )
        if upper_bound is not None and result > upper_bound:
            raise ValueError(
                f"Argument '{name}' needs to be an {int.__name__} <= {upper_bound}. Got: '{result}'({type(result)})"
            )
    return result


def path(
    value: Union[str | pathlib.Path | Any],
    name: str = _DEFAULT_NAME_VALUE,
    *,
    exists: bool = False,
    is_file: bool = False,
    is_dir: bool = False,
    can_write: bool = False,
    can_read: bool = False,
) -> pathlib.Path:
    """
    Validates the value as a valid :py:cls:`pathlib.Path`.

    **NOTE 1**: There is a combinations that do not make sense but is valid:
        ``exists = False`` and ``can_read = True``.
        Reason: why would care about a readable file that doesn't need to exist?

    **NOTE 2:** You also want to avoid the combination ``is_file = True`` and ``is_dir = True``.

    Args:
        value: what to test.
        name: name of the argument (for error reporting). Default: ``"value"``.
        exists: if the path should exist in the file system. Default: py:obj:`False`.
        is_file: if it is a file, if it exists. Default: py:obj:`False`.
        is_dir: if it is a directory, if it exists. Default: py:obj:`False`.
        can_write: the current user has read and write permissions,
          if it does not exist, will check the parent. Default: py:obj:`False`.
        can_read: the current user has read permissions, if it exists. Default: py:obj:`False`.

    Returns:
        the :py:cls:`pathlib.Path` object corresponding to the ``value``.
    """
    # validate type
    value = _path_validate_type_and_return(value, name)
    # validate content
    if exists and not value.exists():
        raise ValueError(f"Argument '{name}'='{value}' must point to an existing path.")
    if value.exists():
        _path_validate_existing(
            value=value, name=name, is_file=is_file, is_dir=is_dir, can_write=can_write, can_read=can_read
        )
    elif can_write and not os.access(value.parent, os.W_OK):
        raise ValueError(f"Argument '{name}'='{value}' parent directory = '{value.parent}' requires write permission.")
    return value


def _path_validate_type_and_return(
    value: Union[str | pathlib.Path | Any],
    name: str = _DEFAULT_NAME_VALUE,
) -> pathlib.Path:
    # validate type
    if isinstance(value, str):
        if not value:
            raise ValueError(f"Argument '{name}'='{value}' cannot be empty.")
        result = pathlib.Path(value).absolute()
    elif isinstance(value, pathlib.Path):
        result = value.absolute()
    else:
        raise TypeError(
            f"Argument '{name}' must be either a '{str.__name__}' or '{pathlib.Path.__name__}' instance. "
            f"Got '{value}'({type(value)})"
        )
    return result


def _path_validate_existing(
    *,
    value: pathlib.Path,
    name: str,
    is_file: bool,
    is_dir: bool,
    can_write: bool,
    can_read: bool,
) -> None:
    # check type
    if is_file and not value.is_file():
        raise ValueError(f"Argument '{name}'='{value}' must point to a regular file.")
    if is_dir and not value.is_dir():
        raise ValueError(f"Argument '{name}'='{value}' must point to a regular directory.")
    # check permissions
    if can_read or can_write:
        if not os.access(value, os.R_OK):
            raise ValueError(f"Argument '{name}'='{value}' requires read permission.")
    if can_write:
        if not os.access(value, os.W_OK):
            raise ValueError(f"Argument '{name}'='{value}' requires write permission.")


def is_callable(
    value: Callable,
    name: str = _DEFAULT_NAME_VALUE,
) -> None:
    """
    Verifies if a particular ``value`` is callable.

    Args:
        value: what to test.
        name: name of the argument (for error reporting). Default: ``"value"``.
    Returns:

    """
    if not callable(value):
        raise TypeError(f"Value of '{name}' must be callable. Got: '{value}'({type(value)})")
