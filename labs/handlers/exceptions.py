class LabsException(Exception):
    message = ""

    def __init__(self, *args, **kwargs):
        super().__init__(self.message.format(*args, **kwargs))


class StreamFileOpeningError(LabsException):
    message = """Exception occured while opening stream for {filename}: {exception}"""


class InvalidPath(LabsException):
    message = """Invalid path provided [{path}]"""


class MissingEnvKeyError(LabsException):
    message = """Environment variable not set for [{key}]"""