#!/usr/bin/env python
"""helpers.dict

Provides methods to browse, flatten, transform complex dict structure,
set values using complex path.
"""


def walk(source, key=None, sep=None):
    """walk

    Recursive function to build dict structure as multiple lists using dict keys
    separated by `sep`.

    Parameters
    ----------
    source : dict
        Dictionary to browse
    key : str
          Starting key
    sep : str
          Key separator to use, default to ':'
    """
    sep = sep or ":"
    key = key or []
    if not isinstance(source, dict):
        return sep.join(key)
    return [walk(source[k], key=key + [k], sep=sep) for k in source.keys()]


def walk_with_value(source, key=None, sep=None):
    """walk_with_value

    Similar behaviour to `walk`, simply associating list of keys to corresponding values.

    Parameters
    ----------
    source : dict
        Dictionary to browse
    key : str
          Starting key
    sep : str
          Key separator to use, default to ':'
    """
    sep = sep or ":"
    key = key or []
    if not isinstance(source, dict):
        return sep.join(key)
    return [
        (walk_with_value(source[k], key=key + [k], sep=sep), value)
        for k, value in source.items()
    ]


def flatten(elements):
    """flatten

    Allows to flatten nested lists into one list.

    Parameters
    ----------

    elements : list
        List of elements to flatten.
    """
    for element in elements:
        if isinstance(element, list):
            yield from flatten(element)
        else:
            yield element


def flatten_with_value(elements):
    """flatten_with_value

    Same behaviour as `flatten` except it adds corresponding value.
    To be used combined with `walk_with_value`.

    Parameters
    ----------
    elements : list
        List of tuples to flatten.
    """
    for element, value in elements:
        if isinstance(element, list):
            yield from flatten_with_value(element)
        else:
            yield element, value


def pathfinder(data, sep=None):
    """pathfinder

    Combined usage of `walk` and `flatten` to build a list of paths found into
    a given dictionary.

    Parameters
    ----------
    data : dict
           Dictionary to walk and flatten to retrieve paths.
    sep : str
          Specific separator to use to build composite keys.
    """
    return list(flatten(walk(data, sep=sep)))


def flatten_dict(data, sep=None):
    """flatten_dict

    Similar behaviour to `flatten` except its using a dictionary as input,
    calling `flatten_with_value`.

    Parameters
    ----------
    data : dict
           Dictionary to flatten.
    sep : str
          Specific separator to use to build composite keys.
    """
    return {k: v for k, v in flatten_with_value(walk_with_value(data, sep=sep))}


def set_deep(_dict, value, keys):
    """set_deep

    Allows to set deep value in dictionary using a list of keys.

    Parameters
    ----------
    _dict : dict
            Dictionary where to set the desired value
    value : object
            Value to be set
    keys : list
           List of keys describing the path to follow to set desired `value`

    """
    lastkey = keys.pop()
    pointer = _dict
    for key in keys:
        pointer = pointer[key]
    pointer[lastkey] = value
    return _dict
