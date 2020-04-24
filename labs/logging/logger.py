#!/usr/bin/env python
"""logging.logger

logging.logger module to handle custom logging settings, defines BaseLogger.
"""

import logging
import logging.config
import os.path
import sys

from dotmap import DotMap
from labs.config import logging as logging_config
from labs.logging.formatter import SimpleFormatter


class BaseLogger(logging.Logger):
    """BaseLogger

    Inherits from logging.Logger to implements specific logger configuration.
    """

    defaults = DotMap(
        name="root",
        loglevel="INFO",
        configfile=os.path.join(os.path.dirname(__file__), "logging.conf"),
        filters=[],
        handlers=[(logging.StreamHandler(sys.stdout), None, SimpleFormatter()),],
    )

    def __init__(self, name=defaults.name):
        if not DotMap.empty(logging_config.handlers.file.filename):
            logging_config.handlers.file.filename = os.path.join(
                os.path.dirname(__file__), logging_config.handlers.file.filename
            )
        logging.config.dictConfig(logging_config.toDict())
        super().__init__(name)
        if DotMap.empty(logging_config.loglevel):
            logging_config.loglevel = self.defaults.loglevel
        loglevel = getattr(logging, logging_config.loglevel)
        self.setup_logger(loglevel)
        self.setup_filters(self.defaults.filters)
        self.setup_handlers(self.defaults.handlers)

    def setup_logger(self, loglevel):
        """setup_logger

        Defines steps to execute to setup logger object.

        Parameters
        ----------
        loglevel : logging.<LOGLEVEL> (logging.INFO, ...)
                   Level to set
        """
        self.setLevel(loglevel)

    def setup_filters(self, filters):
        """setup_filters

        Adds filters to logger object.

        Parameters
        ----------
        filters : list
                  List of logging.Filter objects
        """
        for logfilter in filters:
            self.addFilter(logfilter)

    def setup_handlers(self, handlers):
        """setup_handlers

        Adds handlers to logger object.

        Parameters
        ----------
        handlers : list
                   List of tuples,
                   (
                       logging.Handler object,
                       logging.<LOGLEVEL>,
                       logging.Formatter object,
                   )
        """
        for handler, level, formatter in handlers:
            handler.setLevel(level or self.level)
            handler.setFormatter(formatter)
            self.addHandler(handler)


def getLogger(name, cls=None):  # pylint: disable=invalid-name
    """getLogger

    Simple interface function to simplify possible future logging changes,
    set BaseLogger as default logger class.

    Parameters
    ----------
    name : str
           Logger name
    cls : logging.Logger class
          Logger class to use
    """

    cls = cls or BaseLogger
    logging.setLoggerClass(cls)
    return logging.getLogger(name)


def logger(cls):
    """logger

    Simple decorator to provides logger attributes to given class.

    Parameters
    ----------
    cls: class
    """

    logger_name = "{module}.{cls}".format(module=cls.__module__, cls=cls.__name__)
    cls.logger = getLogger(logger_name)
    return cls
