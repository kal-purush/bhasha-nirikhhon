
import unittest

from anp.formatters import CustomNINJSFormatter


class CustomNINJSFormatterTestCase(unittest.TestCase):

    def test_family_id(self):
        item = {'language': 'en', 'family_id': 'foo', 'type': 'text', 'copyrightholder': 'cp'}
        formatter = CustomNINJSFormatter()
        ninjs = formatter._transform_to_ninjs(item, {})
        self.assertEqual(ninjs['family_id'], item['family_id'])
        self.assertEqual(ninjs['language'], item['language'])

    def test_can_format(self):
        formatter = CustomNINJSFormatter()
        self.assertTrue(formatter.can_format('custom_ninjs', {}))