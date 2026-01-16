"""Send signal characteristic."""

from .characteristic import Characteristic


class TestSendSignalCharacteristic(Characteristic):
    """
    Characteristic that sends a signal to
    the application when its value is written to.

    """
    TEST_SIGNAL_CHRC_UUID = '12345679-1234-5678-1234-56789abcdef1'

    def __init__(self, bus, index, service):
        Characteristic.__init__(
            self, bus, index,
            self.TEST_SIGNAL_CHRC_UUID,
            ['notify'],
            service)