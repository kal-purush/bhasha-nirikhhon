#settings update

import os
import config

from classes.config.config_manager import config_manager

class settings_update():

    def __init__(self):
        self.conf = config.get_conf()

    def run(self):

        config_mng = config_manager()

        download_dir = input('Please enter full path to download folder: ')

        try:
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)

        except Exception as e:
            return[True, False, 'Error while changing download folder to ' + download_dir + '\n' + print(e) + '\n', False]

        self.conf['download_dir'] = download_dir
        config_mng.save(self.conf)

        return[True, False, 'Download folder succesfully changed to ' + self.conf['download_dir'], False]

