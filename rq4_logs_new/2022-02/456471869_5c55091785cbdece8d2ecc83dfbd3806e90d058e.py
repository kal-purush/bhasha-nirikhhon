import sys
import logging
import traceback

import logs.client_log_config
import logs.server_log_config


# Определяем куда писать лог
if sys.argv[0].find('client.py') == -1:
    LOGGER = logging.getLogger('server')
else:
    LOGGER = logging.getLogger('client')


def log(func_to_log):
    def log_saver(*args, **kwargs):
        ret = func_to_log(*args, **kwargs)
        LOGGER.debug(f'Функция {func_to_log.__name__}  с параметрами {args}, {kwargs} была '
                     f'вызвана из функции {traceback.format_stack()[0].strip().split()[-1]}')
        return ret
    return log_saver