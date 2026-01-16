from openhufu.private.utlis.factory import get_cell
from openhufu.private.utlis.defs import CellChannel, CellChannelTopic


class clientDemo():

    def __init__(self, id, cfg):
        self.config = cfg
        self.id = id

    def deploy(self):
        # self.cell = Cell(self.config)
        self.cell = get_cell(self.config)
        self.cell.start()

        self._register_client_cbs()

        self.cell.stop()

    def _finish_handler(self, data):
        print(f"client{self.id} rec finish")
        self.cell.send_message(0, CellChannel.CLIENT_MAIN, CellChannelTopic.Challenge, None)

    def _register_client_cbs(self):
        self.cell.register_request_cb(CellChannel.SERVER_MAIN, CellChannelTopic.Finish, self._finish_handler)