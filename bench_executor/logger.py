#!/usr/bin/env python3

import os
import sys
import logging

LOG_FILE_NAME = 'log.txt'
LOGGER_FORMAT_FILE = '[%(asctime)s] %(levelname)-8s %(message)-s'
LOGGER_FORMAT_CONSOLE = '%(levelname)s: %(message)s'


class Logger:
    def __init__(self, name: str, directory: str, verbose: bool):
        self._logger = logging.getLogger(name)

        level = logging.INFO
        if verbose:
            level = logging.DEBUG
        self._logger.setLevel(level)
        self._logger.handlers = []

        directory = os.path.abspath(directory)
        os.makedirs(directory, exist_ok=True)
        log_file = logging.FileHandler(os.path.join(directory, LOG_FILE_NAME))
        log_file.setLevel(logging.DEBUG)
        format_file = logging.Formatter(LOGGER_FORMAT_FILE)
        log_file.setFormatter(format_file)
        self._logger.addHandler(log_file)

        if os.environ.get('UNITTEST') is None:
            log_console = logging.StreamHandler(sys.stderr)
            log_console.setLevel(logging.WARNING)
            format_console = logging.Formatter(LOGGER_FORMAT_CONSOLE)
            log_console.setFormatter(format_console)
            self._logger.addHandler(log_console)

        level_name = logging.getLevelName(self._logger.level)
        self._logger.info(f'Logger ({level_name}) initialized for {name}')

    def debug(self, msg):
        self._logger.debug(msg)

    def info(self, msg):
        self._logger.info(msg)

    def warning(self, msg):
        self._logger.warning(msg)

    def error(self, msg):
        self._logger.error(msg)
