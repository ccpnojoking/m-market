import os
import re
import random
import inspect
import logging
from logging.handlers import TimedRotatingFileHandler


class Logger:

    def __init__(self, logger_name, logger_file, for_testing=False):
        if not for_testing:
            sudopw = 'vad1996712.'
            self.create_log(logger_file, sudopw)
            self.clean_log(logger_file, sudopw)
        formatter = logging.Formatter(
            '[%(asctime)s]:[%(levelname)s] %(message)s'
        )
        file_handler = TimedRotatingFileHandler(
            logger_file, when='H', interval=8, backupCount=21)
        file_handler.suffix = '%Y%m%d_%H:%M:%S'
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        stream_logger = logging.StreamHandler()
        stream_logger.setLevel(logging.INFO)
        stream_logger.setFormatter(formatter)

        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_logger)

    @staticmethod
    def __get_call_info():
        try:
            stack = inspect.stack()
        except IndexError:
            return "", "", ""
        fn = os.path.basename(stack[2][1])
        ln = stack[2][2]
        func = stack[2][3]
        return fn, func, ln

    @staticmethod
    def __get_msg(call_info, msg):
        return "[%s:%s:L%s] %s" % (call_info[0], call_info[1], call_info[2], msg)

    def info(self, msg, *args, **kwargs):
        call_info = self.__get_call_info()
        msg = self.__get_msg(call_info, msg)
        self.logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        call_info = self.__get_call_info()
        msg = self.__get_msg(call_info, msg)
        self.logger.error(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        call_info = self.__get_call_info()
        msg = self.__get_msg(call_info, msg)
        self.logger.exception(msg, *args, **kwargs)

    def info_ramdon(self, msg, num=100):
        call_info = self.__get_call_info()
        msg = self.__get_msg(call_info, msg)
        if random.randint(1, num) == 1:
            self.logger.info(msg)

    @staticmethod
    def clean_log(logger_file, sudopw):
        dir_name = os.path.dirname(logger_file)
        file_name = logger_file.split('/')[-1]
        files = os.listdir(dir_name)
        files = [file for file in files if re.findall(file_name + '.', file)]
        files.sort()
        files_size = [os.path.getsize(
            os.path.join(dir_name, file)) for file in files]
        while sum(files_size) > 1024 ** 3:
            earliest_file = os.path.join(dir_name, files[0])
            os.system('echo {}|sudo -S {}'.format(sudopw,
                                                  'rm {}'.format(earliest_file)))
            files.pop(0)
            files_size.pop(0)

    @staticmethod
    def create_log(logger_file, sudopw):
        dir_name = os.path.dirname(logger_file)
        if os.path.exists(dir_name):
            os.system('echo {}|sudo -S {}'.format(sudopw,
                                                  'chmod 777 {}'.format(dir_name)))
        else:
            os.system('echo {}|sudo -S {}'.format(sudopw,
                                                  'mkdir -p -m 777 {}'.format(dir_name)))
        if os.path.exists(logger_file):
            os.system('echo {}|sudo -S {}'.format(sudopw,
                                                  'chmod 777 {}'.format(logger_file)))