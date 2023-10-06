# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""Navigates objects or dictionaries using `x-path`_.

.. _x-path: https://en.wikipedia.org/wiki/XPath
"""
from typing import Any, Dict, List, Optional, Tuple

from py_spanner_mutex.common import preprocess

###########################
#  Update Request: paths  #
###########################

REQUEST_PATH_SEP: str = "."


def get_parent_node_based_on_path(value: Any, path: str) -> Tuple[Any, str]:
    """Returns the node and the last key in the path. Example with a
    :py:class:`dict`::

        value = {
            "root": {
                "node_a": 123,
                "node_b": "value_b",
            }
        }
        node, key = get_parent_node_based_on_path(value, "root.node_a")
        assert node == value.get("root")
        assert node.get(key) == 123

    Example with an :py:class:`object`::

        node, key = get_parent_node_based_on_path(value, "root.node_a")
        assert node == value.root
        assert getattr(node, key) == 123

    Args:
        value: either an :py:class:`object` or a :py:class:`dict`.
        path: `x-path`_ like path.

    Returns: :py:class:`tuple` in the format:
        ``<node containing the last key in the path>, <last key in the path>``
    """
    # input validation
    path = preprocess.string(path, "path", strip_it=True, is_empty_valid=False, is_none_valid=False)
    # logic
    return _get_parent_node_attribute_based_on_path_object(value, path)


def _get_parent_node_attribute_based_on_path_object(  # pylint: disable=invalid-name
    value: Any, path: str
) -> Tuple[Any, str]:
    result = value
    split_path = path.split(REQUEST_PATH_SEP)
    for entry in split_path[:-1]:
        result = _get_attribute(result, entry)
        if result is None:
            break
    return result, split_path[-1]


def _get_attribute(value: Any, attr: str) -> Any:
    if isinstance(value, dict):
        result = value.get(attr, None)
    else:
        result = getattr(value, attr, None)
    return result


def create_dict_based_on_path(path: str, value: Any) -> Dict[str, Any]:
    """
    Will create a :py:class:`dict` based on the `path` and `value`.
    Example::
        path = "root.node.subnode"
        value = [1, 2, 3]
        result = create_dict_based_on_path(path, value)
        result = {
            "root": {
                "node": {
                    "subnode": [1, 2, 3]
                }
            }
        }
    Args:
        path:
        value:

    Returns:

    """
    return create_dict_based_on_path_value_lst([(path, value)])


def create_dict_based_on_path_value_lst(  # pylint: disable=invalid-name
    path_value_lst: List[Tuple[str, Optional[Any]]]
) -> Dict[str, Any]:
    """
    Same as :py:func:`create_dict_based_on_path` but multiple times over the same :py:class:`dict`
    Args:
        path_value_lst: list of tuples ``[(<path>,<value>,)]``

    Returns:

    """
    # input validation
    _validate_path_value_lst(path_value_lst, "path_value_lst")
    # logic
    result: Dict[str, Any] = {}
    for path, value in path_value_lst:
        result = _create_dict_based_on_path(result, path, value)
    return result


def _validate_path_value_lst(value: List[Tuple[str, Optional[Any]]], name: str) -> None:
    """Checks if argument is a non-empty :py:class:`list` of :py:class:`tuple`s.

    Args:
        value: what to validate
        name: argument name

    Returns:
    """
    preprocess.validate_type(value, name, list)
    for ndx, path_value in enumerate(value):
        path, _ = path_value
        preprocess.string(path, f"{name}[{ndx}].path", strip_it=True, is_empty_valid=False, is_none_valid=False)


def _create_dict_based_on_path(result: Dict[str, Any], path: str, value: Any) -> Dict[str, Any]:
    node = result
    split_path = path.split(REQUEST_PATH_SEP)
    for entry in split_path[:-1]:
        if entry not in node:
            node[entry] = {}
        node = node[entry]
    node[split_path[-1]] = value
    return result


def _remove_attribute(value: Any, attr: str) -> None:
    if isinstance(value, dict):
        if attr in value:
            del value[attr]
    elif hasattr(value, attr):
        delattr(value, attr)


def remove_keys_based_on_paths(value: Any, paths: List[str]) -> Any:
    """
    It will have side-effects on the input value ``value``.
    It is an in-place change.
    The return is the modified ``value``, just for convenience.
    This means::
        value = { ... }
        paths = [ ... ]
        result = remove_keys_based_on_paths(value, paths)
        assert result == value

    Example::
        value = {
            "root": {
                "node_a": 123,
                "node_b": "value_b",
            }
            "root_lst": [
                {
                    "node_c": 123,
                    "node_d": "value_d_1",
                },
                {
                    "node_c": 321,
                    "node_d": "value_d_2",
                },
                {
                    "node_e": [1, 2, 3],
                },
            ]
        }
        paths = [
            "root.node_a",
            "root_lst[].node_d"
        ]
        result = remove_keys_based_on_paths(value, paths)
        assert value == result
        print(value)
        # output
        {
            "root": {
                "node_a": 123,
            }
            "root_lst": [
                {
                    "node_c": 123,
                },
                {
                    "node_c": 321,
                },
                {
                    "node_e": [1, 2, 3],
                },
            ]
        }
    Args:
        value: target of removals
        paths: what to remove

    Returns:

    """
    # validate input
    preprocess.validate_type(paths, "paths", list)
    # logic
    for ndx, path_entry in enumerate(paths):
        try:
            _remove_keys_based_on_path(value, path_entry)
        except Exception as err:
            raise RuntimeError(
                f"Could not remove path '{path_entry}'[{ndx}] from '{value}'. Paths: '{paths}'. Error: {err}"
            ) from err
    return value


def _remove_keys_based_on_path(value: Any, path: str) -> Any:
    # validate input
    preprocess.string(path, "path")
    # logic
    if "[]" in path:
        first_and_rest_lst_path = path.split("[].", 1)
        node, key = get_parent_node_based_on_path(value, first_and_rest_lst_path[0])
        if node is not None:
            item_lst = _get_attribute(node, key)
            if isinstance(item_lst, list):
                for item in item_lst:
                    _remove_keys_based_on_path(item, first_and_rest_lst_path[1])
    else:
        node, key = get_parent_node_based_on_path(value, path)
        if node is not None:
            _remove_attribute(node, key)
