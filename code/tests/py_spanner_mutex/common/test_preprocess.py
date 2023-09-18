# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,broad-exception-caught
# type: ignore
import os
import pathlib
import re
import stat
import tempfile
from typing import Any, Dict

import pytest

from py_spanner_mutex.common import preprocess

_DEFAULT_VALUE_STR: str = "DEFAULT VALUE"
_DEFAULT_VALUE_INT: int = 123
_TEST_VALUE_STR: str = "test value"
_TEST_VALUE_INT: int = 11
_TEST_VALUE_DICT: dict = {"key": "value"}


@pytest.mark.parametrize(
    "value,cls,is_none_valid,default_value,exp_error,exp_value",
    [
        (None, str, True, None, None, None),  # is None, but should be str. None is acceptable
        (
            None,
            str,
            True,
            _DEFAULT_VALUE_STR,
            None,
            _DEFAULT_VALUE_STR,
        ),  # is None, but should be str. there is a default value
        (
            None,
            str,
            True,
            _DEFAULT_VALUE_INT,
            TypeError,
            None,
        ),  # is None, but should be str. there is a default value, of the wrong type
        (_TEST_VALUE_STR, str, False, None, None, _TEST_VALUE_STR),  # happy path
        (_TEST_VALUE_STR, str, False, _DEFAULT_VALUE_INT, None, _TEST_VALUE_STR),  # happy path
        (_TEST_VALUE_INT, int, False, None, None, _TEST_VALUE_INT),  # happy path
        (_TEST_VALUE_DICT, dict, False, None, None, _TEST_VALUE_DICT),  # happy path
        (_TEST_VALUE_INT, str, True, None, TypeError, None),  # is an int, but should be str
        (_TEST_VALUE_INT, str, False, None, TypeError, None),  # is an int, but should be str
    ],
)
def test_validate_type(value: Any, cls: type, is_none_valid: bool, default_value: Any, exp_error: type, exp_value: Any):
    try:
        result = preprocess.validate_type(
            value, "test_name", cls, is_none_valid=is_none_valid, default_value=default_value
        )
        assert result == exp_value
    except Exception as err:
        if not exp_error:
            raise err
        assert isinstance(err, exp_error)


@pytest.mark.parametrize(
    "value,strip_it,is_empty_valid,is_none_valid,default_value,exp_error,exp_value",
    [
        ("", False, True, False, None, None, ""),  # is empty and accept empty
        (" ", False, False, False, None, None, " "),  # not empty, but blank
        (None, True, False, True, None, None, None),  # None, but acceptable
        (None, False, False, False, _DEFAULT_VALUE_STR, None, _DEFAULT_VALUE_STR),  # None, but has default value
        ("", True, False, False, None, ValueError, None),  # empty and not acceptable
        (" ", True, False, False, None, ValueError, None),  # blank and not acceptable
        (None, True, True, False, None, TypeError, None),  # None and not acceptable
        (_TEST_VALUE_INT, True, True, True, _DEFAULT_VALUE_INT, TypeError, None),  # not a string and not acceptable
        (" to_strip ", True, False, False, _DEFAULT_VALUE_STR, None, "to_strip"),  # get stripped version
    ],
)
def test_string_simple(
    value: str,
    strip_it: bool,
    is_empty_valid: bool,
    is_none_valid: bool,
    default_value: Any,
    exp_error: type,
    exp_value: Any,
):
    try:
        result = preprocess.string(
            value,
            "test_name",
            strip_it=strip_it,
            is_empty_valid=is_empty_valid,
            is_none_valid=is_none_valid,
            default_value=default_value,
        )
        assert result == exp_value
    except Exception as err:
        if not exp_error:
            raise err
        assert isinstance(err, exp_error)


@pytest.mark.parametrize(
    "value,regex,exp_error,exp_value",
    [
        (123, ".*", TypeError, None),
        ("", ".*", ValueError, None),
        (None, ".*", TypeError, None),
        (" ", ".*", ValueError, None),
        ("ac", "^a.*c$", None, "ac"),
        ("ac", re.compile("^a.*c$"), None, "ac"),
        ("abc", "^a.*c$", None, "abc"),
        ("aacbbbcaac", re.compile("^a.*c$"), None, "aacbbbcaac"),
    ],
)
def test_string_regex(
    value: str,
    regex: str,
    exp_error: type,
    exp_value: Any,
):
    try:
        result = preprocess.string(
            value,
            "test_name",
            regex=regex,
        )
        assert result == exp_value
    except Exception as err:
        if not exp_error:
            raise err
        assert isinstance(err, exp_error)


@pytest.mark.parametrize(
    "value,lower_bound,upper_bound,is_none_valid,default_value,exp_error,exp_value",
    [
        (None, 2, 1, True, None, RuntimeError, None),  # lower > upper
        (None, None, None, True, None, None, None),  # is None but acceptable
        (None, None, None, False, _DEFAULT_VALUE_INT, None, _DEFAULT_VALUE_INT),  # is None but has default value
        (None, None, None, False, _DEFAULT_VALUE_STR, TypeError, None),  # is None and default value is wrong type
        (
            None,
            _DEFAULT_VALUE_INT + 1,
            _DEFAULT_VALUE_INT + 2,
            False,
            _DEFAULT_VALUE_INT,
            ValueError,
            None,
        ),  # is None and default value outside bounds
        (None, 1, 2, True, None, None, None),  # is None but acceptable
        (1, 1, 2, False, None, None, 1),  # happy path
        (2, 1, 2, False, None, None, 2),  # happy path
        (0, 1, 2, False, None, ValueError, None),  # value < lower_bound
        (3, 1, 2, False, None, ValueError, None),  # value > upper_bound
        ("1", 1, 2, False, None, TypeError, None),  # not an int
    ],
)
def test_integer(
    value: Any,
    lower_bound: int,
    upper_bound: int,
    is_none_valid: bool,
    default_value: Any,
    exp_error: type,
    exp_value: Any,
):
    try:
        result = preprocess.integer(
            value,
            "test_name",
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            is_none_valid=is_none_valid,
            default_value=default_value,
        )
        assert result == exp_value
    except Exception as err:
        if not exp_error:
            raise err
        assert isinstance(err, exp_error)


# pylint: disable=consider-using-with
_TEST_REGULAR_RW_FILE: str = tempfile.NamedTemporaryFile(mode="w+b", delete=False).name
_TEST_REGULAR_RO_FILE: str = tempfile.NamedTemporaryFile(mode="rb", delete=False).name
_TEST_REGULAR_NOT_EXIST_FILE: str = tempfile.NamedTemporaryFile(delete=True).name
_TEST_REGULAR_RW_DIR: str = tempfile.NamedTemporaryFile(mode="rb", delete=False).name
# pylint: enable=consider-using-with

_MODE_FILE_RO: int = stat.S_IRUSR | stat.S_IRGRP
_MODE_FILE_RW: int = _MODE_FILE_RO | stat.S_IWUSR
_MODE_DIR_RO: int = stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
_MODE_DIR_RW: int = _MODE_DIR_RO | stat.S_IWUSR

_TEST_ROOT_DIR: str = tempfile.mkdtemp()


def _create_parent_dir() -> str:
    result = tempfile.mkdtemp(dir=_TEST_ROOT_DIR)
    return result


def _create_dir(exists: bool = True, mode: int = _MODE_DIR_RW, parent_mode: int = _MODE_DIR_RW) -> str:
    parent = _create_parent_dir()
    result = tempfile.mkdtemp(dir=parent)
    # exist
    if not exists:
        os.rmdir(result)
    else:
        os.chmod(result, mode)
    # permissions parent
    os.chmod(parent, parent_mode)
    #
    return result


def _create_file(exists: bool = True, mode: int = _MODE_FILE_RW, parent_mode: int = _MODE_DIR_RW) -> str:
    parent = _create_parent_dir()
    result = tempfile.NamedTemporaryFile(dir=parent, delete=False).name  # pylint: disable=consider-using-with
    # exist
    if not exists:
        os.remove(result)
    else:
        os.chmod(result, mode)
    # permissions parent
    os.chmod(parent, parent_mode)
    #
    return result


@pytest.mark.parametrize(
    "kwargs,error",
    [
        (  # Happy path RW file
            {
                "value": _create_file(exists=True, mode=_MODE_FILE_RW),
                "exists": True,
                "is_file": True,
                "can_write": True,
                "can_read": True,
            },
            None,
        ),
        (  # Happy path RO file
            {
                "value": str(_create_file(exists=True, mode=_MODE_FILE_RO, parent_mode=_MODE_DIR_RO)),
                "exists": True,
                "is_file": True,
                "can_read": True,
            },
            None,
        ),
        (  # Happy path RW dir
            {
                "value": _create_dir(exists=True, mode=_MODE_DIR_RW),
                "exists": True,
                "is_dir": True,
                "can_write": True,
                "can_read": True,
            },
            None,
        ),
        (  # Happy path RO dir
            {
                "value": _create_dir(exists=True, mode=_MODE_DIR_RO, parent_mode=_MODE_DIR_RO),
                "exists": True,
                "is_dir": True,
                "can_read": True,
            },
            None,
        ),
        (  # Happy path file does NOT exist
            {
                "value": str(_create_file(exists=False, mode=_MODE_FILE_RW)),
                "exists": False,
                "is_file": True,
                "can_write": True,
                "can_read": True,
            },
            None,
        ),
        (  # Happy path dir does NOT exist
            {
                "value": _create_dir(exists=False, mode=_MODE_DIR_RW),
                "exists": False,
                "is_dir": True,
                "can_write": True,
                "can_read": True,
            },
            None,
        ),
        (  # File does not exist, must be RW, but parent is RO
            {
                "value": _create_file(exists=False, mode=_MODE_FILE_RW, parent_mode=_MODE_DIR_RO),
                "exists": False,
                "is_file": True,
                "can_write": True,
                "can_read": True,
            },
            ValueError,
        ),
        (  # Dir does not exist, must be RW, but parent is RO
            {
                "value": _create_dir(exists=False, mode=_MODE_DIR_RW, parent_mode=_MODE_DIR_RO),
                "exists": False,
                "is_dir": True,
                "can_write": True,
                "can_read": True,
            },
            ValueError,
        ),
        (  # File exists, must be RW, but is RO
            {
                "value": _create_file(exists=True, mode=_MODE_FILE_RO),
                "exists": True,
                "is_file": True,
                "can_write": True,
            },
            ValueError,
        ),
        (  # Dir exists, must be RW, but is RO
            {
                "value": _create_dir(exists=False, mode=_MODE_DIR_RO),
                "exists": True,
                "is_dir": True,
                "can_write": True,
            },
            ValueError,
        ),
        (  # Value is wrong type
            {"value": None},
            TypeError,
        ),
        (  # Value is wrong type
            {"value": 123},
            TypeError,
        ),
        (  # Value is wrong type
            {"value": ""},
            ValueError,
        ),
    ],
)
def test_path(kwargs: Dict[str, Any], error: Any):
    try:
        result = preprocess.path(**kwargs)
        assert result == pathlib.Path(kwargs["value"])
    except Exception as err:
        if not error:
            raise err
        assert isinstance(err, error)


@pytest.mark.parametrize(
    "value,is_error",
    [
        (lambda: None, False),
        (preprocess.is_callable, False),
        (object(), True),
        ("value", True),
        (123, True),
    ],
)
def test_is_callable(value: Any, is_error: bool):
    try:
        preprocess.is_callable(value)
        result = True
    except Exception:
        result = False
    assert result is not is_error
