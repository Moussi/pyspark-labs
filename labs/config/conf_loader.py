from labs.config.file import File

from dotmap import DotMap
from pyhocon import ConfigFactory


class HoconLoader:

    config_file = File.name_from_env("config")

    def load(cls, config_file):
        config_file = config_file or cls.config_file
        return DotMap(ConfigFactory.parse_file(config_file))
