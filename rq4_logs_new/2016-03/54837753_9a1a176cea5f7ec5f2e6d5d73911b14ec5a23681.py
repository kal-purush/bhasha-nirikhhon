import os
import sys
import unittest
from datetime import datetime

from ConfigParser import SafeConfigParser
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+'/../')
from generate_countdown_image import apply_text_on_image, format_time_simpler, remaining_time

class TestCsvToJson(unittest.TestCase):

    def test_control(self):
        self.assertTrue(True, 'Control test successful!')

    def test_makes_image(self):
        conf = SafeConfigParser()
        conf.read('test/settings.cfg')

        username = conf.get('reddit', 'username')
        password = conf.get('reddit', 'password')
        subreddit = conf.get('reddit', 'subreddit')
        target = conf.get('image', 'target')
        image_location = 'test/header.png'
        font = conf.get('image', 'font')
        posx = conf.get('image', 'posx')
        posy = conf.get('image', 'posy')

        target_datetime = datetime.strptime(target, '%Y %m %d %H:%M:%S')
        timediff = remaining_time(target_datetime)

        result = apply_text_on_image(image_location, format_time_simpler(timediff), font, {'x':posx, 'y':posy})
        result.save('test/result.png')

if __name__ == '__main__':
    unittest.main()
    