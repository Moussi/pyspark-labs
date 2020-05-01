import functools
import pathlib
import shutil
import os
import pyarrow as pa
from labs.config.env import Env
from labs.config import labs
from labs.handlers.exceptions import MissingEnvKeyError


class FsContext:

    def fs_from_env(method):
        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            try:
                key = method(*args, **kwargs)
                if Env().name == "local":
                    return LocalFS()
                else:
                    return Hdfs()
            except KeyError:
                raise MissingEnvKeyError(key=key)

        return wrapper

    @property
    @fs_from_env
    def fs(self):
        return "ENV_CURRENT"

    def succeeded(self, path):
        pass

    @classmethod
    def exists(cls, log_date, fs=None):
        path = cls.output_parquet_path_from_log_date(log_date)
        if fs is not None:
            return fs.succeeded(path)
        else:
            with FsContext() as fs:
                return fs.succeeded(path)


class LocalFS(FsContext):
    config = labs.localfs
    server_name = "hadoop_data"

    def __init__(self, config=None):
        self.config = config or self.config

    @staticmethod
    def delete(path, recursive=False):
        path = pathlib.Path(path)
        if recursive:
            shutil.rmtree(path=path)
        else:
            path.rmdir() if not os.path.isfile(path) else path.unlink()

    def succeeded(self, path):
        path = '{path}/_SUCCESS'.format(path=path)

        path = pathlib.Path(path)
        return True if pathlib.Path.exists(path) else False


class Hdfs(FsContext):
    config = labs.hdfs
    server_name = "hadoop_data"

    def __init__(self, config=None, server=None, server_name=None, port=None):
        self.config = config or self.config
        # driver = driver or "libhdfs"
        server_name = server_name or self.server_name
        server = server or self.config.servers[server_name]
        port = port or server.port
        self.conn = pa.hdfs.connect(
            server.addr, port=port, user=server.user
        )

    def delete(self, path, recursive=False):
        self.conn.delete(path, recursive=recursive)

    def succeeded(self, path):
        path = '{path}/_SUCCESS'.format(path=path)
        return self.conn.exists(path)
