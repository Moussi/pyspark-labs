
from labs.config.conf_loader import HoconLoader
from labs.config.file import File


class Config:
    def load(self, name, loader=None, prefix=None):
        loader = loader or HoconLoader
        file_name = File.name_from_env(name=name)
        config_file = File.as_relative_path(name=file_name)
        return loader.load(config_file=config_file)


spark = Config().load("spark")
labs = Config().load("labs")
