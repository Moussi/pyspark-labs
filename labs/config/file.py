import pathlib

from labs.config.env import Env


class File:
    prefix = "config"

    @classmethod
    def name_from_env(cls, name, env=None, ext="conf"):
        return f"{name}.{env or Env().name}.{ext}"

    @classmethod
    def as_relative_path(cls, name, prefix=None):
        prefix = prefix or cls.prefix
        path = pathlib.Path(__file__).parent.parent / prefix / name
        return str(path)

