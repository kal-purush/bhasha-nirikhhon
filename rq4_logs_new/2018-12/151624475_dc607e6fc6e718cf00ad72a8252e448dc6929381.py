import unittest
import db


class TestStringMethods(unittest.TestCase):

    def test_rbg(self):
        self.assertEqual(db.rbg, ['27,255,128','104,36,1','94,43,0'
                                  '255,153,25','195,55,60','252,37,94'
                                  '243,1,48','255,180,24','255,251,19',
                                  '254,214,2','0,115,208','1,18,232'
                                  '10,0,131','3,3,3','13,255,0'
                                  '0,0,0','89,89,89'])

    def test_paint(self):
        self.assertEqual(db.retreive_colors(['27,255,128']),  ['Bright Aqua Green', 'https://www.michaels.com/liquitex-basics-acrylic-paint-8.5oz/MD002595S.html?dwvar_MD00'
                                                                                    '2595S_size=8.5%20oz&dwvar_MD002595S_color=Cadmium%20Red%20Medium%20Hue#prefn1=re'
                                                                                    'finementColor&pmpt=qualifying&pref', '27,255,128'])


if __name__ == '__main__':
    unittest.main()