"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01
"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler

DEBUG = 10
LOG_PATH = os.getcwd() + '/log'

class LogHandler(logging.Logger):
    """
    customed logger
    """

    def __init__(self, name, level=DEBUG, stream=True, file=True):
        self.name = name
        self.level = level

        logging.Logger.__init__(self, self.name, level=level)
        if stream: self.__setStreamHandler__()
        if file: self.__setFileHandler__()

    def __setFileHandler__(self, level=None):
        """
        set file handler
        :param level:
        :return:
        """

        file_name = os.path.join(LOG_PATH, '{name}.log'.format(name=self.name))
        file_handler = TimedRotatingFileHandler(filename=file_name, when='D', interval=1, backupCount=3)
        file_handler.suffix = '%Y-%m-%d.log'
        if not level:
            file_handler.setLevel(self.level)
        else:
            file_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        file_handler.setFormatter(formatter)

        self.file_handler = file_handler
        self.addHandler(file_handler)

    def __setStreamHandler__(self, level=None):
        """
        set stream handler
        :param level:
        :return:
        """

        stream_handler = logging.StreamHandler()
        if not level:
            stream_handler.setLevel(self.level)
        else:
            stream_handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        stream_handler.setFormatter(formatter)

        self.addHandler(stream_handler)