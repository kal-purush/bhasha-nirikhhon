from PyQt5.QtCore import pyqtSignal, pyqtSlot

from model.alchemy.client import YandexClient
from model.api_items.yandex import YaAPIDirectClient
from tasks.db.common import PQDBTask


class LoadClients(PQDBTask):
    got_clients = pyqtSignal(list)

    def __init__(self):
        super().__init__()

    def run(self):
        try:
            YandexClient.load_json(self.session)
            self.got_clients.emit([
                YaAPIDirectClient(
                    login=client.login,
                    token=client.token,
                    timestamp=client.timestamp,
                    is_active=client.set_active
                )
                for client in self.session.query(YandexClient).all()
            ])
        except Exception as e:
            self.error_occurred.emit(e)
