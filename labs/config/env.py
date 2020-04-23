import functools
import os

from labs.handlers.exceptions import MissingEnvKeyError


class Env:
    def from_env(method):
        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            try:
                key = method(*args, **kwargs)
                return os.environ[key]
            except KeyError:
                return "local"
                #raise MissingEnvKeyError(key=key)

        return wrapper

    @property
    @from_env
    def name(self):
        return "ENV_CURRENT"
