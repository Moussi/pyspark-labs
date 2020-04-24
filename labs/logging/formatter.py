"""logging.formatter

logging.formatter defines specific logging.Formatter classes.
"""

import logging


class SimpleFormatter(logging.Formatter):
    """SimpleFormatter

    Defines base logging format using logging.Formatter class.
    """

    datefmt = "%y/%m/%d %H:%M:%S"
    fmt = "%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s"

    def __init__(self, fmt=None, datefmt=None, style="%"):
        datefmt = datefmt or self.datefmt
        super().__init__(fmt=fmt or self.fmt, datefmt=datefmt, style=style)
