import functools
import pathlib

from labs.handlers.exceptions import InvalidPath


class PathWrapper:
    @classmethod
    def exists(cls, method):

        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            path = kwargs.get("path")
            path = pathlib.Path(path)
            if not pathlib.Path.exists(path):
                raise InvalidPath(path=path)

            return method(*args, **kwargs)
        return wrapper
