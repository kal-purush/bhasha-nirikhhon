from .modules import Server
from .modules import Database
from .modules import Scheduler
from .modules import Logger, LoggerHandlers
from .modules import Hawk

from .messengers import Telegram

class Core:

    def __init__(self, params):
        """
        Initiate Core with startup params.

        :param port integer: Port for web server
        :param name string: Bot's identifier for DB
        :param hawk string: Hawk project token
        """

        # TODO check params
        self.PARAMS = params

        self.logger = Logger(self).logger

        self.db = Database().get(self.PARAMS['name'])

        # TODO remove
        if self.PARAMS.get('hawk'):
            self.hawk = Hawk(self.PARAMS['hawk']).hawk
            LoggerHandlers.add(self, self.logger, ['hawk'])

        LoggerHandlers.add(self, self.logger, ['journal'])
        # LoggerHandlers.add(self, self.logger, ['db'])

        self.server = Server(self.PARAMS['port'])

        self.scheduler = Scheduler(self)

        self.telegram = Telegram(self.PARAMS['telegram_token'])