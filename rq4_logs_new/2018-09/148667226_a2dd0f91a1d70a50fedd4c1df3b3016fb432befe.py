import unittest


class NameStoreTests(unittest.TestCase):
    def _makeOne(self):
        from docbasesync.langhelpers import NameStore
        return NameStore()

    def test_access_by_same_key(self):
        target = self._makeOne()
        one = object()
        target[one] = "foo"
        self.assertEqual(target[one], "foo")
        target[one] = "foo"
        self.assertEqual(target[one], "foo")

    def test_access_by_not_same_keys(self):
        target = self._makeOne()
        one = object()
        two = object()
        three = object()

        target[one] = "foo"
        self.assertEqual(target[one], "foo")

        target[two] = "foo"
        self.assertEqual(target[two], target.new_name("foo", 1))
        self.assertEqual(target[two], "foo01")

        target[three] = "foo"
        self.assertEqual(target[one], "foo")
        self.assertEqual(target[two], "foo01")
        self.assertEqual(target[three], "foo02")

    def test_access_different_positions(self):
        target = self._makeOne()
        one = object()
        two = object()
        target[one] = "foo"
        self.assertEqual(target[one], "foo")
        target[two] = "bar"
        self.assertEqual(target[two], "bar")