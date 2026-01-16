from spittalmodel import SpittalModel
from spittalexposure import SpittalExposure
from spittalrun import SpittalRun
import logging

class SpittalPond():
    """ Python interface to the Oasis Django API.

    This class provides an easy to use front-end interface to the Oasis
    mid-tier Django API.
    """

    def __init__(self, base_url, user,
                 log_file=None, log_level=logging.INFO):
        """ Initiate with server URL and user.

        Args:
            base_url (str): The URL of the Django server. Be sure to prepend
                the protocol (i.e. http://) and append the port (i.e. :8000).
            user (str): Username to use on the server.
        """

        logger = logging.getLogger('spittalpond')
        if log_file == None:
            logger.disabled = True
        else:
            logger.propagate = False
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(log_level)

        logger.info('Initating new spittalpond instance.')
        self.model = SpittalModel(base_url, user)
        self.exposure = SpittalExposure(base_url, user)
        self.run = SpittalRun(base_url, user)