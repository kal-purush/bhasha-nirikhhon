import unittest
from monarch import Config, Rule

class RulesTest(unittest.TestCase):

    def setUp(self):
        self.config = Config(host='localhost', port='6379', db=0)
        self.config.namespace = "dnd_test"
        self.config.cache_size = 99
        self.rule = Rule(self.config, pattern="%s:r:c:%s:t:%s")

    def tearDown(self):
        keys = self.rule.list()
        for key in keys:
            self.config.redis.delete(key)

    def test_rule_add(self):
        self.assertEqual(self.rule.add("notfication", "promo", 60), True)

    def test_invalid_rule_add(self):
        with self.assertRaises(TypeError):
            self.rule.add(None, None, 60)

    def test_construct_rule_key(self):
        self.assertEqual(self.rule.key("channel", "category"), "dnd_test:r:c:channel:t:category")

    def test_get_rule(self):
        self.rule.add("notification", "promo", 60)

        self.assertNotEqual(self.rule.get("notification", "promo"), None)
        self.assertEqual(self.rule.get("notification", "promo"), '60')

    def test_get_non_existent_rule(self):
        self.assertEqual(self.rule.get("unknown", "promo"), None)

    def test_get_list_of_rules(self):
        self.rule.add("notfication", "promo", 60)
        self.rule.add("email", "promo", 60)
        self.rule.add("sms", "promo", 60)

        self.assertEqual(len(self.rule.list()), 3)

    def test_remove_rules(self):
        self.rule.add("notfication", "promo", 60)
        self.rule.add("email", "promo", 60)
        self.rule.add("sms", "promo", 60)
        self.rule.remove("sms", "promo")

        self.assertEqual(len(self.rule.list()), 2)

if __name__ == '__main__':
    unittest.main()