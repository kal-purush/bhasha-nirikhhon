# -*- coding: utf-8 -*-

from ivy.std_api import *
from optparse import OptionParser
import logging
import signal
from pyrejeu.clock import RejeuClock
from pyrejeu.importations import RejeuImportation

logging.basicConfig(format='%(asctime)-15s - %(levelname)s - %(message)s', level=logging.DEBUG)


if __name__ == "__main__":
    # importation du fichier
    import_obj = RejeuImportation()

    # gestion des options
    # connection au bus ivy

    clock = RejeuClock()
    # gestion des signaux
    def handler(signum, frame):
        clock.stop()
        IvyStop()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    clock.run()

