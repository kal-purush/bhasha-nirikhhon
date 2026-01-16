def binary_search(list, item):
    start = 0
    end = len(list) - 1
    while start <= end:
        middle = (start + end) // 2
        guess = list[middle]

        if item == guess:
            return guess

        if item < guess:
            end = middle - 1

        else:
            start = middle + 1

    return None


import unittest
class BinarySearchTest(unittest.TestCase):
    
    fib_list = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    
    def test_binary_search_success(self):
        self.assertEqual(5, binary_search(self.fib_list, 5))

    def test_binary_search_failue(self):
        self.assertEqual(None, binary_search(self.fib_list, 6))


if __name__ == "__main__":
    unittest.main()