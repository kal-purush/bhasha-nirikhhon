import unittest
import main as mn



class TestMain(unittest.TestCase):

    def test_get_hash(self):
        """
        Тест функции get_hash: с помощью неё вычисляем хэш для строки "some password" и сравниваем с заранее определённым значением
        """
        self.assertEqual(mn.get_hash("some password".encode("utf-8")), "e62e1269317b9654e1314dfecb78f29b35ad4d362da0a9c2ccdb680aa535d7ea")
        
if __name__ == "__main__":
    unittest.main()